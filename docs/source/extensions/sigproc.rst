ezmsg-sigproc
=============

Timeseries signal processing implementations in ezmsg, leveraging numpy and scipy.
Most of the methods and classes in this extension are intended to be used in building signal processing pipelines.
They use :class:`ezmsg.util.messages.axisarray.AxisArray` as the primary data structure for passing signals between components.
The message's data are expected to be a numpy array.

Note for offline processing: some generators might yield valid :class:`AxisArray` messages with ``.data`` size of 0.
This may occur when the generator receives inadequate data to produce a valid output, such as when windowing or buffering.

ezmsg.sigproc.activation
-----------------------------

.. automodule:: ezmsg.sigproc.activation
    :members:


ezmsg.sigproc.affinetransform
-----------------------------

.. automodule:: ezmsg.sigproc.affinetransform
    :members:


ezmsg.sigproc.aggregate
-----------------------

.. automodule:: ezmsg.sigproc.aggregate
    :members:
    :undoc-members:


ezmsg.sigproc.bandpower
-----------------------

.. automodule:: ezmsg.sigproc.bandpower
    :members:


ezmsg.sigproc.filter
--------------------

.. automodule:: ezmsg.sigproc.filter
    :members:

ezmsg.sigproc.butterworthfilter
-------------------------------

.. automodule:: ezmsg.sigproc.butterworthfilter
    :members:


ezmsg.sigproc.decimate
----------------------

.. automodule:: ezmsg.sigproc.decimate
    :members:


ezmsg.sigproc.downsample
------------------------

.. automodule:: ezmsg.sigproc.downsample
    :members:


ezmsg.sigproc.ewmfilter
-----------------------

.. automodule:: ezmsg.sigproc.ewmfilter
    :members:


ezmsg.sigproc.math
-----------------------

.. automodule:: ezmsg.sigproc.math.clip
    :members:

.. automodule:: ezmsg.sigproc.math.difference
    :members:

.. automodule:: ezmsg.sigproc.math.invert
    :members:

.. automodule:: ezmsg.sigproc.math.log
    :members:

.. automodule:: ezmsg.sigproc.math.scale
    :members:


ezmsg.sigproc.sampler
---------------------

.. automodule:: ezmsg.sigproc.sampler
    :members:


ezmsg.sigproc.scaler
--------------------

.. automodule:: ezmsg.sigproc.scaler
    :members:


ezmsg.sigproc.signalinjector
----------------------------

.. automodule:: ezmsg.sigproc.signalinjector
    :members:


ezmsg.sigproc.slicer
---------------------

.. automodule:: ezmsg.sigproc.slicer
    :members:


ezmsg.sigproc.spectrum
----------------------

.. automodule:: ezmsg.sigproc.spectrum
    :members:
    :undoc-members:


ezmsg.sigproc.spectrogram
-------------------------

.. automodule:: ezmsg.sigproc.spectrogram
    :members:


ezmsg.sigproc.synth
-------------------

.. automodule:: ezmsg.sigproc.synth
    :members:


ezmsg.sigproc.window
--------------------

.. automodule:: ezmsg.sigproc.window
    :members:
