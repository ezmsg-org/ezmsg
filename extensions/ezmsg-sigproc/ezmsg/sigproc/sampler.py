from dataclasses import dataclass, replace, field
import logging
import time

import ezmsg.core as ez
import numpy as np

from .messages import TSMessage

from typing import Optional, Any, Tuple, List, Dict

@dataclass( frozen = True )
class SampleTriggerMessage:
    timestamp: float = field( default_factory = time.time )
    period: Optional[ Tuple[ float, float ] ] = None
    value: Any = None

@dataclass
class SampleMessage:
    trigger: SampleTriggerMessage
    sample: TSMessage

class SamplerSettings( ez.Settings ):
    buffer_dur: float
    period: Optional[ Tuple[ float, float ] ] = None # Optional default period if unspecified in SampleTriggerMessage
    value: Any = None # Optional default value if unspecified in SampleTriggerMessage

    estimate_alignment: bool = True
    # If true, use message timestamp fields and reported sampling rate to estimate 
    # sample-accurate alignment for samples.
    # If false, sampling will be limited to incoming message rate -- "Block timing"
    # NOTE: For faster-than-realtime playback --  Incoming timestamps must reflect
    # "realtime" operation for estimate_alignment to operate correctly.

class SamplerState( ez.State ):
    triggers: Dict[ SampleTriggerMessage, int ] = field( default_factory = dict )
    last_msg: Optional[ TSMessage ] = None
    buffer: Optional[ np.ndarray ] = None

class Sampler( ez.Unit ):
    SETTINGS: SamplerSettings
    STATE: SamplerState

    INPUT_TRIGGER = ez.InputStream( SampleTriggerMessage )
    INPUT_SIGNAL = ez.InputStream( TSMessage )
    OUTPUT_SAMPLE = ez.OutputStream( SampleMessage )

    @ez.subscriber( INPUT_TRIGGER )
    async def on_trigger( self, msg: SampleTriggerMessage ) -> None:
        if self.STATE.last_msg is not None:
            fs = self.STATE.last_msg.fs

            period = msg.period if msg.period is not None else self.SETTINGS.period
            value = msg.value if msg.value is not None else self.SETTINGS.value

            if period is None:
                ez.logger.warning( f'Sampling failed: period not specified' )
                return

            # Check that period is valid
            start_offset = int( period[0] * fs )
            stop_offset = int( period[1] * fs )
            if ( stop_offset - start_offset ) <= 0:
                ez.logger.warning( f'Sampling failed: invalid period requested' )
                return

            # Check that period is compatible with buffer duration
            max_buf_len = int( self.SETTINGS.buffer_dur * fs )
            req_buf_len = int( ( period[1] - period[0] ) * fs )
            if req_buf_len >= max_buf_len:
                ez.logger.warning( f'Sampling failed: {period=} >= {self.SETTINGS.buffer_dur=}' )
                return

            offset: int = 0
            if self.SETTINGS.estimate_alignment:
                # Do what we can with the wall clock to determine sample alignment
                wall_delta = msg.timestamp - self.STATE.last_msg.timestamp
                offset = int( wall_delta * fs )

            # Check that current buffer accumulation allows for offset - period start
            if -min( offset + start_offset, 0 ) >= self.STATE.buffer.shape[0]:
                ez.logger.warning( 'Sampling failed: insufficient buffer accumulation for requested sample period' )
                return

            trigger = replace( msg, period = period, value = value )
            self.STATE.triggers[ trigger ] = offset 

        else: ez.logger.warning( 'Sampling failed: no signal to sample yet' )

    @ez.subscriber( INPUT_SIGNAL )
    @ez.publisher( OUTPUT_SAMPLE )
    async def on_signal( self, msg: TSMessage ) -> None:

        if self.STATE.last_msg is None:
            self.STATE.last_msg = msg

        # Easier to deal with timeseries on axis 0
        last_msg = self.STATE.last_msg
        msg_data = np.swapaxes( msg.data, msg.time_dim, 0 )
        last_msg_data = np.swapaxes( last_msg.data, last_msg.time_dim, 0 )
        
        if ( # Check if signal properties have changed in a breaking way
            msg.fs != last_msg.fs or \
            msg.time_dim != last_msg.time_dim or \
            msg_data.shape[1:] != last_msg_data.shape[1:]
        ):
            # Data stream changed meaningfully -- flush buffer, stop sampling
            if len( self.STATE.triggers ) > 0:
                ez.logger.warning( 'Sampling failed: Discarding all triggers' )
            ez.logger.warning( 'Flushing buffer: signal properties changed' )
            self.STATE.buffer = None
            self.STATE.triggers = dict()

        # Accumulate buffer ( time dim => dim 0 )
        self.STATE.buffer = msg_data if self.STATE.buffer is None else \
            np.concatenate( ( self.STATE.buffer, msg_data ), axis = 0 )

        pub_samples: List[ SampleMessage ] = []
        remaining_triggers: Dict[ SampleTriggerMessage, int ] = dict()
        for trigger, offset in self.STATE.triggers.items():

            # trigger_offset points to t = 0 within buffer
            offset = offset - msg.n_time
            start = offset + int( trigger.period[0] * msg.fs )
            stop = offset + int( trigger.period[1] * msg.fs )

            if stop < 0: # We should be able to dispatch a sample
                sample_data = self.STATE.buffer[ start : stop, ... ]
                sample_data = np.swapaxes( sample_data, msg.time_dim, 0 )
                
                pub_samples.append( SampleMessage( 
                    trigger = trigger,
                    sample = replace( msg, data = sample_data )
                ) )

            else: remaining_triggers[ trigger ] = offset

        for sample in pub_samples:
            yield self.OUTPUT_SAMPLE, sample 
            
        self.STATE.triggers = remaining_triggers

        buf_len = int( self.SETTINGS.buffer_dur * msg.fs )
        self.STATE.buffer = self.STATE.buffer[ -buf_len:, ... ]
        self.STATE.last_msg = msg


## Dev/test apparatus
import asyncio

from ezmsg.testing.debuglog import DebugLog
from ezmsg.sigproc.synth import Oscillator, OscillatorSettings

from typing import AsyncGenerator

class SampleFormatter( ez.Unit ):

    INPUT = ez.InputStream( SampleMessage )
    OUTPUT = ez.OutputStream( str )

    @ez.subscriber( INPUT )
    @ez.publisher( OUTPUT )
    async def format( self, msg: SampleMessage ) -> AsyncGenerator:
        str_msg = f'Trigger: {msg.trigger.value}, '
        str_msg += f'{msg.sample.n_time} samples @ {msg.sample.fs} Hz, '
        
        time_axis = np.arange(msg.sample.n_time) / msg.sample.fs
        time_axis = time_axis + msg.trigger.period[0]
        str_msg += f'[{time_axis[0]},{time_axis[-1]})'
        
        yield self.OUTPUT, str_msg

class TriggerGeneratorSettings( ez.Settings ):
    period: Tuple[ float, float ] # sec
    prewait: float = 0.5 # sec
    publish_period: float = 5.0 # sec

class TriggerGenerator( ez.Unit ):

    SETTINGS: TriggerGeneratorSettings

    OUTPUT_TRIGGER = ez.OutputStream( SampleTriggerMessage )

    @ez.publisher( OUTPUT_TRIGGER )
    async def generate( self ) -> AsyncGenerator:
        await asyncio.sleep( self.SETTINGS.prewait )

        output = 0
        while True:
            yield self.OUTPUT_TRIGGER, SampleTriggerMessage(
                period = self.SETTINGS.period,
                value = output
            )

            await asyncio.sleep( self.SETTINGS.publish_period )
            output += 1

class SamplerTestSystemSettings( ez.Settings ):
    sampler_settings: SamplerSettings
    trigger_settings: TriggerGeneratorSettings

class SamplerTestSystem( ez.System ):

    SETTINGS: SamplerTestSystemSettings

    OSC = Oscillator()
    SAMPLER = Sampler()
    TRIGGER = TriggerGenerator()
    FORMATTER = SampleFormatter()
    DEBUG = DebugLog()

    def configure( self ) -> None:
        self.SAMPLER.apply_settings( self.SETTINGS.sampler_settings )
        self.TRIGGER.apply_settings( self.SETTINGS.trigger_settings )

        self.OSC.apply_settings( OscillatorSettings(
            n_time = 2, # Number of samples to output per block
            fs = 10,  # Sampling rate of signal output in Hz
            dispatch_rate = 'realtime',
            freq = 2.0,  # Oscillation frequency in Hz
            amp = 1.0,  # Amplitude
            phase = 0.0,  # Phase offset (in radians)
            sync = True, # Adjust `freq` to sync with sampling rate
        ) )

    def network( self ) -> ez.NetworkDefinition:
        return (
            ( self.OSC.OUTPUT_SIGNAL, self.SAMPLER.INPUT_SIGNAL ),
            ( self.TRIGGER.OUTPUT_TRIGGER, self.SAMPLER.INPUT_TRIGGER ),

            ( self.TRIGGER.OUTPUT_TRIGGER, self.DEBUG.INPUT ),
            ( self.SAMPLER.OUTPUT_SAMPLE, self.FORMATTER.INPUT ),
            ( self.FORMATTER.OUTPUT, self.DEBUG.INPUT )
        )

if __name__ == '__main__':

    settings = SamplerTestSystemSettings( 
        sampler_settings = SamplerSettings(
            buffer_dur = 5.0
        ),
        trigger_settings = TriggerGeneratorSettings(
            period = ( 1.0, 2.0 ),
            prewait = 0.5,
            publish_period = 5.0
        )
    )

    system = SamplerTestSystem( settings )

    ez.run_system( system )