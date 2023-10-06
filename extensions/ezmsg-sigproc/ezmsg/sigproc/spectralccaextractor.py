import ezmsg.core as ez
import numpy as np
import asyncio 

from sklearn.cross_decomposition import CCA
from ezmsg.util.messages.axisarray import AxisArray #from ezmsg.eeg.eegmessage import EEGMessage
from typing import (
    Optional,
    AsyncGenerator,
    List
) 
from dataclasses import field, dataclass, replace

@dataclass
class TransformOutput:
    output: float

class SpectralCcaSettings(ez.Settings):
# Set our frequencies of interest to be selected from, and harmonics we'd like to check.
    freqoi: List[float] = field( default_factory = list([7, 9, 13]) )
    n_harm: int = 3
    timedim: str = 'time'

class SpectralCcaState(ez.State):
    sampFreq: Optional[float] = None #

class SpectralCcaExtractor(ez.Unit):
    """
    Performs a CCA on data collected from EEG channels.
    """
    SETTINGS: SpectralCcaSettings
    STATE: SpectralCcaState
    
    # We dont know what they'll be named, we need to take in additional setting to tell us which
    # axis is the time axis, flattening the representation and everything in the datastream not the time axis is
    # a channel. That setting is timedim  
    INPUT_SIGNAL = ez.InputStream(AxisArray) 
    OUTPUT_DECODE = ez.OutputStream(TransformOutput)
    @ez.subscriber(INPUT_SIGNAL)
    @ez.publisher(OUTPUT_DECODE)
    async def extract(self, msg: AxisArray) -> AsyncGenerator:
        # n_components here are the number of distinct frequencies we would like to search for
        # correlations with the incoming data of the BCI (3 in this case).
        cca = CCA(n_components=3)
        # Time formed from the message 'n_time' data (1, 2, 3...) and dividing by
        # sampling frequency (500 Hz).[0.000, 0.002, 0.004,...]
        #
        # Harmonics desired for checking correlations is from settings (3) and can be played
        # with in the future to adjust ITR or accuracy.
        #TODO: Need to see if this is correct below. Im following along from spectral.py
        time = msg.ax(self.SETTINGS.timedim).values
        harm_idx = (np.arange(self.SETTINGS.n_harm)+1)
        
        # Main code block.
        # allcores will contain all the correlation coefficients produced by sklearn CCA funtion
        # for the (3) frequencies of interest.
        # Y contains all of the sine and cosine functions and their harmonics for each FoI. Y_n,
        # where n is the FoI, should be of size (# of time points, 2*harmonics). # of time points
        # can be changed by the Window size chosen in our System settings.
        # Since the fit_transform function does not output correlations, but outputs the the
        # covariance matrices maximizing the relationship between msg.data and Y, we use
        # np.corrcoef to calculate and add the correlation values to allcores.
        allcores = []
        for f in self.SETTINGS.freqoi:
            Y =[]
            for h in harm_idx:
                Y.append(np.sin(2*np.pi*f*h*time))
                Y.append(np.cos(2*np.pi*f*h*time))
            Y = np.array(Y).T
            A, B = cca.fit_transform(msg.view2d(self.SETTINGS.timedim), Y ) #we're using view2D to 
            #flatten the data along the time axis 0, and every other axis is flattened to axis 1 
            corrs = [np.corrcoef(A[:, i], B[:, i])[0, 1] for i in range(cca.n_components)]
            allcores.append(corrs[0])

        max_freq = self.SETTINGS.freqoi[np.argmax(allcores)]
        
        yield (self.OUTPUT_DECODE, TransformOutput(max_freq))