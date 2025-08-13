Leveraging ezmsg For Signal Processing
###############################################

(under construction)
Ezmsg is a powerful framework for building signal processing applications. It provides a flexible and extensible architecture that allows users to create custom signal processors, integrate with ezmsg Units, and build complex processing pipelines.

We will explore how to do this by recreating the `downsample` signal processor unit. It will demonstrate how to create a signal processor, convert it to an ezmsg Unit, and use it in a processing pipeline.

.. tip: downsampling is a common signal processing operation that reduces the sampling rate of a signal by keeping only every nth sample. This is useful for reducing the amount of data to be processed, especially in real-time applications.


Choosing your signal processing class
*************************************

We make use of the following decision tree to choose the appropriate signal processing class:

include decision tree here

downsample is a bla bla, so we will create a . 


Creating the downsample signal processor
****************************************


DownSampleSettings Class
====================================

To create a downsample signal processor, we first define the settings for the processor. This includes parameters such as the downsampling factor and any other relevant configuration options


DownSampleState Class
=====================================


DownSample Class
======================================

This class will inherit from the appropriate ezmsg signal processor base class and implement the necessary methods to perform the downsampling operation. The class will also define input and output ports for the signal processor.


Input and Output streams
***************************************

The `DownSample` class will define input and output streams to handle the data flow. The input stream will receive the signal data, and the output stream will send the downsampled data.


From/to a device
========================================


From/to AxisArray messages
========================================




Using AxisArray messages
**********************************

The `DownSample` class can also work with AxisArray messages for more complex data structures. This allows for greater flexibility in handling multi-dimensional signals.


The final DownSample signal processor
*****************************************

The `DownSample` class is now fully implemented and ready for use in signal processing pipelines. It can efficiently downsample signals, handle various input and output formats, and integrate with other ezmsg components.
```python
class DownSampleSettings(ezmsg.Settings):
    def __init__(self, downsample_factor=2):
        super().__init__()
        self.downsample_factor = downsample_factor
class DownSampleState(ezmsg.State):
    def __init__(self):
        super().__init__()
        self.current_index = 0
class DownSample(ezmsg.SignalProcessor):
    def __init__(self, settings: DownSampleSettings):
        super().__init__(settings)
        self.input_port = ezmsg.Port("input", ezmsg.AxisArray)
        self.output_port = ezmsg.Port("output", ezmsg.AxisArray)
        self.state = DownSampleState()
    def process(self, data: ezmsg.AxisArray):
        downsampled_data = ezmsg.AxisArray()
        for i in range(0, data.size(), self.settings.downsample_factor):
            downsampled_data.append(data[i])
        self.output_port.send(downsampled_data)



Creating the DownSample ``ezmsg`` Unit
***************************************

We utilise the in-built Signal processing unit classes in `ezmsg-sigproc` to convert our `DownSample` class into an `ezmsg` Unit. This allows us to easily integrate the signal processor into the `ezmsg` framework and use it in processing pipelines.

```python
class DownSampleUnit(ezmsg.Unit):
    def __init__(self, settings: DownSampleSettings):
        super().__init__(settings)
        self.processor = DownSample(settings)
    def process(self, data: ezmsg.AxisArray):
        self.processor.process(data)
class DownSampleUnitSettings(ezmsg.UnitSettings):
    def __init__(self, downsample_factor=2):
        super().__init__()
        self.downsample_factor = downsample_factor
class DownSampleUnitState(ezmsg.UnitState):
    def __init__(self):
        super().__init__()
        self.current_index = 0

Further Examples
***********************

The `Examples <https://github.com/iscoe/ezmsg/tree/master/examples>`_ directory is a great place to start. The homepage also has a link to a Google Colab notebook.


See Also
****************************************

- `ezmsg-sigproc` documentation
- Signal processing tutorials
- `DownSample` class reference