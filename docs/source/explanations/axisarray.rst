About AxisArray
######################

``AxisArray`` is a specialized message format used within the `ezmsg` framework to represent multi-dimensional data structures. In simple terms they are **N-dimensional arrays with labeled axes and optional metadata**. It is designed for applications in signal processing, scientific computing, and data analysis, where both the data and its context are important. ``AxisArray`` originally took inspiration from the functionality of the `xarray <https://docs.xarray.dev/en/stable/>`_ library. 

``AxisArray`` is built-in to ezmsg. Import using:

.. code-block:: python

    from ezmsg.core.util import AxisArray

.. warning:: Importing ``AxisArray`` from ``ezmsg.core.util`` will import the `numpy` library. For this reason, we have implemented ezmsg in such a way that if you do not import ``AxisArray``, `numpy` will not be imported either. This is ideal for users wanting very lightweight applications and have no need for the functionality of `numpy`. 

|ezmsg_logo_small| Why use AxisArray?
****************************************

The purpose of including `AxisArray` as part of ezmsg stems from wanting to avoid having to support a vast array of different message types. This is a major cause of bloat with other similar messaging platforms. 

For this to be useful, we have designed `AxisArray` to be convenient and flexible. It is important that it can be used for the many different use cases we have encountered. At its core it stores a numpy N-dimensional array, an array of axis labels, as well as multiple metadata attributes. 


|ezmsg_logo_small| Description
*********************************

An ``AxisArray`` is a multi-dimensional array with named axes. Each axis can have a name and a set of labels for its elements. This allows for more intuitive indexing and manipulation of the data.

An `AxisArray` has the following attributes:

- ``data``: a numpy ndarray containing the actual data.
- ``dims``: a list of axis names.
- ``axes``: a dictionary mapping axis names to their label information.
- ``attrs``: a dictionary for storing additional metadata.
- ``key``: a unique identifier for the array.

Unsurprisingly, all of this must be self-consistent: the number of axis names in ``dims`` must match the number of dimensions in ``data``, and the axis names in ``axes`` should match the ones in ``dims``.  The label information in ``axes`` refers to the 'value' of each axis index, e.g., for a time axis, the labels might be timestamps. We provide three commonly used axes type objects:

- A ``LinearAxis``: represents a linear axis with evenly spaced values - you just need the ``offset`` (start value) and the ``gain`` (step size). An example of this would be simple numerical index (offset=0, gain=1) or regularly spaced time samples (offset=start time, gain=1/sampling rate).
- A ``TimeAxis``: this is a `LinearAxis` that represents a time axis. Its ``unit`` attribute is by default set to seconds (s).
- A ``CoordinateAxis``: this is our continuous/dense axis, which can represent any continuous variable, such as frequency or spatial coordinates. You provide the actual values for each index in a ``data`` array of values.

The `AxisArray` class provides several methods for manipulating and accessing the data, designed to allow standard numpy array manipulation without losing track of how the axes are affected. For a list of such methods, see :ref:`axisarray_methods`. 


|ezmsg_logo_small| Recommended Use
**********************************************


When to use AxisArray
=======================

If any of the following are true, then `AxisArray` is an appropriate messaging format:

- When the underlying data is in array form
- When it is prudent to keep information about the data array axes (eg. labels, units, spacing)
- When you want to store the data with additional metadata (eg. data origin device name, study name, date, attempt number, etc.)
- If you plan to use the signal processing extension package `ezmsg-sigproc`. 

Anything that is inherently multi-dimensional and needs to be processed in a multi-dimensional context, ``AxisArray`` is the preferred message format in ezmsg. We have designed the ezmsg signal processing extension package `ezmsg-sigproc` with `AxisArray` in mind.

We do not recommend using `AxisArray` for very simple data types like integers, floats, or strings. `AxisArray` has the flexibility to store this kind of data: this can be done by sending the data as a zero-dimensional array, or by sending an empty data field and storing the simple data in the ``attrs`` field. However, for the sake of both memory and time efficiency, we would recommend simply using the basic types (int, float, str) as the message type instead.

.. tip:: One example of a convenient use of the metadata components of `AxisArray` is in the case of adaptive transformers: we need to be able to send trigger messages that tell the adaptive transformer to change state/configuration in some way. We can do this by having an element of ``attrs`` that is called ``"trigger"`` set to ``True`` along with whatever parameters of the transformer we would like to alter (and any data needed for computation of the new parameters). The transformer can then check for this ``"trigger"`` attribute in the incoming `AxisArray` messages and change its state accordingly.

.. _axisarray_methods:

`AxisArray` utility methods
=============================

There are many built in methods for manipulating `AxisArray` objects, such as:

- `as2d`: Get a 2D view of the data with the specified dimension as the first axis.
- `shape2d`: Calculate the 2D shape when viewing array with specified axis first.
- `slice_along_axis`: Slice the input array along a specified axis using the given slice object or integer index.
- `sliding_win_oneaxis`: Generates a view of an array using a sliding window of specified length along a specified axis of the input array.
- `view2d`: Context manager providing a 2D view of the data.

The following are AxisArray class methods for manipulating the underlying data:

- `concatenate`: Concatenate multiple AxisArray objects along a specified dimension.
- `isel`: Select data using integer-based indexing along specified dimensions.
- `iter_over_axis`: Iterate over slices of the data along a specified axis.
- `sel`: Select data using label-based indexing along specified dimensions.
- `to_xr_dataarray`: Convert the AxisArray to an xarray DataArray.
- `transpose`: Transpose the dimensions of an AxisArray.

Further utility methods (for getting data views, axis information, and shapes):

- `as2d`: Get a 2D view of the data with the specified dimension as the first axis.
- `ax`: Get `AxisInfo` for a specified dimension.
- `axis_idx`: Get the axis index for a given dimension name or pass through if already an int.
- `get_axis`: Get the axis coordinate system for a specified dimension.
- `get_axis_idx`: Get the axis index for a given dimension name.
- `get_axis_name`: Get the axis name for a given axis index.
- `shape`: Get the shape of the data array.
- `shape_2d`: Get the shape of the 2D view of the data with the specified dimension as the first axis.
- `view_2d`: Context manager providing a 2D view of the data.


How to return an AxisArray object
=================================

To return an ``AxisArray`` object, you can create an instance of the ``AxisArray`` class and populate it with your data. 

There is some time cost in the creation of the ``AxisArray`` object. For performance-critical code, the preferred way is to use ``replace`` (imported from **ezmsg.util.messages.axisarray**) to modify the data as needed before returning the ``AxisArray`` object. One can think of this as similar to how one would use ``dataclasses.replace`` to create a new instance of a dataclass with some attributes changed.

.. code-block:: python

    from ezmsg.util.messages.axisarray import LinearAxis, replace

    # Create some sample data
    new_data = some_processing_function(message.data)

    # Define newly created axis
    axis = LinearAxis(offset=0.0, gain=0.01, unit='s')

    # Create a new AxisArray message by replacing data and axes
    msg_out = replace(
        message,
        data=new_data,
        axes={
            **message.axes,
            new_axis: axis,
        },
    )

Calling ``ezmsg.util.messages.axisarray.replace()`` calls the utility function ``fast_replace`` (in `ezmsg.util.messages.util`) which is an optimized version of the standard python ``dataclasses.replace`` function. The optimization occurs due to skipping certain validation checks that would normally occur when initialising a dataclass. Specifically, unlike ``dataclasses.replace``, this function does not check for type compatibility, nor does it check that the passed in fields are valid fields for the dataclass and not flagged as ``init=False``.

If you have concerns over this reduced safety, if you set the environment variable ``EZMSG_DISABLE_FAST_REPLACE=1``, then this imported ``replace`` function will simply be the function ``dataclasses.replace`` defined in the python standard `dataclasses` module. 

.. note:: Use of this purpose-made ``replace`` function is not limited to ``AxisArray`` objects. It can be used to create any dataclass object given an instance of said class, including user-defined dataclasses. An example of this can be seen in the tutorial :ref:`here <processing_data_tutorial>`.

|ezmsg_logo_small| See Also
********************************

#. :doc:`../reference/API/axisarray`
#. :doc:`sigproc`
#. :doc:`../tutorials/signalprocessing`

.. :doc:`../how-tos/axisarray/content-axisarray`

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo