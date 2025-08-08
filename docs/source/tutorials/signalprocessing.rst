Leveraging ezmsg For Signal Processing
###############################################

`ezmsg` is a powerful framework for building signal processing applications. It provides a flexible and extensible architecture that allows users to create custom signal processors, integrate with ezmsg Units, and build complex processing pipelines.

We will explore how to do this by recreating the `Downsample` signal processor unit. It will demonstrate how to create a signal processor, convert it to an ezmsg Unit, and use it in a processing pipeline. Additionally, it will provide a mini primer on the `AxisArray` class, which is the preferred ezmsg message format.

.. tip:: Downsampling is a common signal processing operation that reduces the sampling rate of a signal by keeping only every nth sample. This is useful for reducing the amount of data to be processed, especially in real-time applications.


|ezmsg_logo_small| Choosing your signal processing class
**********************************************************

We make use of the following decision tree to choose the appropriate signal processing class:

.. graphviz::
    :align: center

    digraph signal_processor_decision_tree {
        node [shape=box, style="rounded,filled", fillcolor="#f0f0f0", fontname="Arial"];
        edge [fontname="Arial"];

        AMP [label="Multiple Processors?", fontcolor="#ff0000"];
        ARI [label="Receives Input?", fontcolor="#ff0000"];
        ACB [label="Single Chain / Branching?"];
        P [label="Producer", shape=diamond, fillcolor="#27f21cff"];
        APO [label="Produces Output?", fontcolor="#ff0000"];
        NBC [label="no base class", style="none"];
        ACRI [label="Receives Input?"];
        C [label="Consumer", shape=diamond, fillcolor="#27f21cff"];
        T [label="Transformer", shape=diamond, fillcolor="#27f21cff", fontcolor="#ff0000"];
        PS [label="Stateful?"];
        CS [label="Stateful?"];
        TS [label="Stateful?", fontcolor="#ff0000"];
        TSA [label="Adaptive?", fontcolor="#ff0000"];
        TSAF [label="Async First?", fontcolor="#ff0000"];
        CompositeProducer [style="none, filled", fillcolor="#effb1aff"];
        CompositeProcessor [style="none, filled", fillcolor="#effb1aff"];
        BaseProducer [style="none, filled", fillcolor="#effb1aff"];
        BaseStatefulProducer [style="none, filled", fillcolor="#effb1aff"];
        BaseConsumer [style="none, filled", fillcolor="#effb1aff"];
        BaseStatefulConsumer [style="none, filled", fillcolor="#effb1aff"];
        BaseTransformer [style="none, filled", fillcolor="#effb1aff"];
        BaseAdaptiveTransformer [style="none, filled", fillcolor="#effb1aff"];
        BaseStatefulTransformer [style="none, filled", fillcolor="#effb1aff", fontcolor="#ff0000"];
        BaseAsyncTransformer [style="none, filled", fillcolor="#effb1aff"];

        AMP -> ARI [label="no", color="#ff0000", fontcolor="#ff0000"];
        AMP -> ACB [label="yes"];
        ARI -> P [label="no"];
        ARI -> APO [label="yes", color="#ff0000", fontcolor="#ff0000"];
        ACB -> NBC [label="branching"];
        ACB -> ACRI [label="single chain"];
        P -> PS;
        APO -> C [label="no"];
        APO -> T [label="yes", color="#ff0000", fontcolor="#ff0000"];
        ACRI -> CompositeProducer [label="no"];
        ACRI -> CompositeProcessor [label="yes"];
        PS -> BaseProducer [label="no"];
        PS -> BaseStatefulProducer [label="yes"];
        C -> CS;
        T -> TS [color="#ff0000", fontcolor="#ff0000"];
        CS -> BaseConsumer [label="no"];
        CS -> BaseStatefulConsumer [label="yes"];
        TS -> BaseTransformer [label="no"];
        TS -> TSA [label="yes", color="#ff0000", fontcolor="#ff0000"];
        TSA -> TSAF [label="no", color="#ff0000", fontcolor="#ff0000"];
        TSA -> BaseAdaptiveTransformer [label="yes"];
        TSAF -> BaseStatefulTransformer [label="no", color="#ff0000", fontcolor="#ff0000"];
        TSAF -> BaseAsyncTransformer [label="yes"];
    }
.. flowchart TD
..     AMP{Multiple Processors?};
..     AMP -->|no| ARI{Receives Input?};
..     AMP -->|yes| ACB{Single Chain / Branching?}
..     ARI -->|no| P(Producer);
..     ARI -->|yes| APO{Produces Output?};
..     ACB -->|branching| NBC[no base class];
..     ACB -->|single chain| ACRI{Receives Input?};
..     P --> PS{Stateful?};
..     APO -->|no| C(Consumer);
..     APO -->|yes| T(Transformer);
..     ACRI -->|no| CompositeProducer;
..     ACRI -->|yes| CompositeProcessor;
..     PS -->|no| BaseProducer;
..     PS -->|yes| BaseStatefulProducer;
..     C --> CS{Stateful?};
..     T --> TS{Stateful?};
..     CS -->|no| BaseConsumer;
..     CS -->|yes| BaseStatefulConsumer;
..     TS -->|no| BaseTransformer;
..     TS -->|yes| TSA{Adaptive?};
..     TSA -->|no| TSAF{Async First?};
..     TSA -->|yes| BaseAdaptiveTransformer;
..     TSAF -->|no| BaseStatefulTransformer;
..     TSAF -->|yes| BaseAsyncTransformer;

In our case, we are creating a **single** signal processor that **receives input** and **produces output**. The decision tree indicates that we will be using a **transformer**-type base class. To continue, we need to determine if the processor is *stateful*, *adaptive* and *async first* or not.

A stateful processor maintains internal state information that can affect its processing behavior, while a stateless processor does not maintain any internal state and processes each input independently. Adaptive transformers are a subtype of transformer that can adjust its settings based on trigger messages, whereas all other transformers are non-adaptive. Async first transformers prioritise asynchronous processing, meaning they can handle incoming messages without blocking, while non-async first transformers may block while processing messages.

To answer whether our `Downsample` transformer is any of these types, we need to identify what we consider the settings (configuration) for the transformer and what we consider the state.

A good rule of thumb is that settings are parameters used to configure the processor and are typically set once during initialization and remain constant. On the other hand, the processor state is internal data that the processor needs to maintain during its operation and can change dynamically as the processor processes data. 

We will see that `Downsample` is stateful, not adaptive and not async first, so we will inherit from the `BaseStatefulTransformer` class. This will become clearer as we implement the processor in the following sections.

First, we need to install the `ezmsg-sigproc` package if we haven't already. This package contains the base classes for signal processing in ezmsg. You can install it using pip:

.. code-block:: bash

    pip install "ezmsg[sigproc]"


|ezmsg_logo_small| Creating the `Downsample` signal processor
*************************************************************

We begin by identifying the components needed to create the `Downsample` signal processor. This includes defining the settings, state, and the main processing class itself.

First create a new Python file named `downsample.py` in your root directory. In this file we will implement the `Downsample` signal processor.

Add the following import statements to the top of the `downsample.py` file:

.. code-block:: python

    # downsample.py
    import numpy as np
    from ezmsg.util.messages.axisarray import (
        AxisArray,
        slice_along_axis,
        replace,
    )
    import ezmsg.core as ez

    from ezmsg.sigproc.base import (
        BaseStatefulTransformer,
        BaseTransformerUnit,
        processor_state,
    )

.. note:: These are modules we will need in the implementation and will be explained as we go along. You will notice that we import `numpy` (for numerical operations), `AxisArray` (this is our class for handling multi-dimensional arrays with named axes), and from `ezmsg-sigproc`, we import the `BaseStatefulTransformer` class and the `BaseTransformerUnit` (for wrapping our processor into an ezmsg unit). 


DownsampleSettings class
====================================

To create a `Downsample` signal processor, we first define the settings for the processor. The parameters that we need to know for the transformer to operate include: 

- the axis along which to downsample.
- desired rate after downsampling has occurred, or
- the desired factor by which to downsample. 

Thus, your settings class will look like this:

.. code-block:: python

    class DownsampleSettings(ez.Settings):
        """
        Settings for :obj:`Downsample` node.
        """

        axis: str = "time"
        """The name of the axis along which to downsample."""

        target_rate: float | None = None
        """Desired rate after downsampling. The actual rate will be the nearest integer factor of the input rate that is the same or higher than the target rate."""

        factor: int | None = None
        """Explicitly specify downsample factor.  If specified, target_rate is ignored."""

There are no ``__init__`` methods that you might expect because we are inheriting from ``ez.Settings``, which uses Python's dataclass functionality to automatically generate the ``__init__`` method based on the class attributes.

.. tip:: It is very good practice to name your settings class with the name of your processor followed by `Settings`. This makes it easy to identify the settings class for a given processor. 

The fact that we will not ever need to change these settings implies we do not need use of an adaptive transformer.  

DownsampleState class
========================

For the general operation of the `Downsample` processor, we need to keep track of the downsampling factor (since this could change per message) and the index of the next message's first sample (for maintaining continuity in the downsampled output), especially when processing a stream of data.

The fact that we need to maintain state information implies that we will need to use a stateful transformer. 

Your state class will look like this:

.. code-block:: python

    @processor_state
    class DownsampleState:
        q: int = 0
        """The integer downsampling factor. It will be determined based on the target rate."""

        s_idx: int = 0
        """Index of the next msg's first sample into the virtual rotating ds_factor counter."""

Again, our class seems to be missing an ``__init__`` method, but this is because we are using the ``@processor_state`` decorator from `ezmsg-sigproc`, which automatically generates the ``__init__`` method for us. Just another way to make our code cleaner and more maintainable.

.. note:: It is very good practice to name your state class with the name of your processor followed by `State`. This makes it easy to identify the state class for a given processor. 

.. note:: Finally, our transformer is **not async first** as we do not need to prioritise asynchronous processing, which is usually more relevant for processors that interface with IO operations whose timing is unpredictable.

|ezmsg_logo_small| DownsampleTransformer Class
*******************************************************

We have already identified that we will be using a stateful transformer, so we will inherit from the ``BaseStatefulTransformer`` class. Create the class definition as follows:

.. code-block:: python

    class DownsampleTransformer(
        BaseStatefulTransformer[DownsampleSettings, AxisArray, AxisArray, DownsampleState]
    ):
        """
        Downsampled data simply comprise every `factor`th sample.
        This should only be used following appropriate lowpass filtering.
        If your pipeline does not already have lowpass filtering then consider
        using the :obj:`Decimate` collection instead.
        """

        def _hash_message(self, message: AxisArray) -> int: ...

        def _reset_state(self, message: AxisArray) -> None: ...

        def _process(self, message: AxisArray) -> AxisArray: ...

.. note:: The `BaseStatefulTransformer` class is a generic class that takes four type parameters: the settings type, the input message type, the output message type, and the state type. In our case, the settings type is `DownsampleSettings`, the input and output message types are both `AxisArray`, and the state type is `DownsampleState`.


As can be seen above we must implement the following methods:

- ``_hash_message``: This method is used to generate a hash for the input message. This is useful for caching and avoiding redundant processing.
- ``_reset_state``: This method is used to reset the internal state of the processor. This is useful when starting a new processing session or when the input data changes significantly.
- ``_process``: This is the main processing method where the downsampling logic will be implemented.

The first two methods deal with the state of the processor (and are only required for stateful processors), while the third method is where the actual downsampling logic will be implemented. 

.. important:: ``_process`` is a necessary method for all transformers and consumers. The equivalent method for producers is called ``_produce``. For non-stateful processors, this will be the only method you need to implement if you inherit from the relevant base class. All other methods are preimplemented for you, but you can override them if needed.

In order to implement these methods, we need to understand our preferred message format: `AxisArray`. This is a flexible and powerful class for handling multi-dimensional arrays with named axes, which is particularly useful for signal processing applications. I have already used `AxisArray` in our code as the input message and output message types.

A detailed explanation of the `AxisArray` class is beyond the scope of this tutorial, but you can refer to the :doc:`AxisArray explainer <../explanations/axisarray>` as well as the :doc:`API reference <../reference/API/axisarray>` for more information.

Brief Aside on AxisArray
=================================

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

The `AxisArray` class provides several methods for manipulating and accessing the data, and the one we will be using in our `Downsample` processor is ``slice_along_axis``. This method allows us to slice the array along a specified axis, which is essential for downsampling.

Hashing the State
===========================

We can generate a unique hash for the input message using the `key` attribute of the `AxisArray` which we tend to use for identifying what device our data has come from as well as an identifier of the message structure (in this case, the `gain` of the axes containing the data). Since downsampling requires messages to come with linearly spaced data, our axes will either be a `LinearAxis` or a `TimeAxis`, so this attribute will exist. 

Our implementation of the ``_hash_message`` method will look like this:

.. code-block:: python

    def _hash_message(self, message: AxisArray) -> int:
        return hash((message.axes[self.settings.axis].gain, message.key))

.. note:: The idea here is that if either the gain of the axis or the key of the message changes, we are dealing with different data, so we need to reevaluate our state. Importantly, the `DownsampleTransformer` *can* be implemented in a stateless way, but this would require computing the downsampling factor and first sample index every time, and hence a much less efficient implementation.


Resetting the State
=================================

The ``_reset_state`` method is used to reset the internal state of the processor when a message is received with a hash different than that stored by the `DownsampleTransformer`. We need to reset the downsampling factor and the index of the next message's first sample. This is important when starting a new processing session or when the input data changes shape (like a different sampling rate).

We set the downsampling factor either to the one in `DownsampleSettings` if specified, else we compute it based on the target rate and the input message rate. If target rate is not specified, we default to a downsampling factor of 1 (no downsampling). If a target rate is specified, we compute the downsampling factor as the nearest integer that is the same or higher than the ratio of the input rate to the target rate. If the final downsampling factor is less than 1 (not a valid value), we set it to 1 (no downsampling).

Finally, we reset the index of the next message's first sample to 0.

.. code-block:: python

    def _reset_state(self, message: AxisArray) -> None:
        axis = message.get_axis(self.settings.axis)

        if self.settings.factor is not None:
            q = self.settings.factor
        elif self.settings.target_rate is None:
            q = 1
        else:
            q = int(1 / (axis.gain * self.settings.target_rate))
        if q < 1:
            ez.logger.warning(
                f"Target rate {self.settings.target_rate} cannot be achieved with input rate of {1 / axis.gain}."
                "Setting factor to 1."
            )
            q = 1
        self._state.q = q
        self._state.s_idx = 0


.. _processing_data_tutorial:

|ezmsg_logo_small| Processing the Data
***********************************************

To finish the `DownsampleTransformer` class, we need to actually process the data by downsampling. 
This is done in the ``_process`` method. We will use some of the methods provided by the `AxisArray` class to help us with this.

Step 1: Getting the indices to slice the data
=========================================================

We first get the index of the axis (`axis_idx`) and the axis itself (`axis`) along which we want to downsample. We then determine the number of samples in the input message along that axis:

.. code-block:: python

    downsample_axis = self.settings.axis
    axis = message.get_axis(downsample_axis)
    axis_idx = message.get_axis_idx(downsample_axis)
    n_samples = message.data.shape[axis_idx]

Next, create a linear range of indices starting from the current index of the next message's first sample (`self._state.s_idx`) to the current index plus the number of samples in the input message. We use modulo operation with the downsampling factor (`self._state.q`) to create a virtual rotating counter. If the number of samples is greater than 0, we update the index of the next message's first sample for the next iteration. Our slice object is the indices where the virtual counter is 0, which corresponds to the samples we want to keep after downsampling:

.. code-block:: python

    samples = (
        np.arange(self.state.s_idx, self.state.s_idx + n_samples) % self._state.q
    )
    if n_samples > 0:
        # Update state for next iteration.
        self._state.s_idx = samples[-1] + 1

    pub_samples = np.where(samples == 0)[0]
    if len(pub_samples) > 0:
        n_step = pub_samples[0].item()
        data_slice = pub_samples
    else:
        n_step = 0
        data_slice = slice(None, 0, None)

Here `pub_samples` corresponds to the samples we want to keep after downsampling - they are the zeros in our virtual counter. If there are any samples to publish, we set `n_step` to the first index in `pub_samples` (ie. the first zero) and `data_slice` to `pub_samples`. If there are no samples to publish, we set `n_step` to 0 and `data_slice` to an empty slice.

Step 2: Slicing the data and updating the axis
=========================================================

We will create the output message by first creating our new numpy ndarray by slicing the input message's data along the specified axis using the `slice_along_axis` function from the `AxisArray` class. Then we will update the axis information to reflect the downsampling. Finally, we create a new `AxisArray` message with the downsampled data and updated axes using the ``replace`` function from the `AxisArray` class.

The slicing of the data is done as follows:

.. code-block:: python

    slice_along_axis(message.data, sl=data_slice, axis=axis_idx)


We also need to update the axis information to reflect the downsampling. All other axes stay as before, but the one we downsampled on (`downsample_axis`) needs to be updated. The gain of the axis is multiplied by the downsampling factor, and the offset is updated based on the number of steps taken in the virtual counter:

.. code-block:: python

    from ezmsg.util.messages.axisarray import replace

    new_axes={
        **message.axes,
        downsample_axis: replace(
            axis,
            gain=axis.gain * self._state.q,
            offset=axis.offset + axis.gain * n_step,
        ),
    }

.. important:: The ``replace`` function is a utility function provided by the `AxisArray` class that allows us to create a new object with updated attributes while keeping the other attributes unchanged. It is very fast by avoiding deep copies of the entire object and safety checks that usually occur at object creation time. Its signature is ``replace(obj: T, **changes) -> T``, where `obj` is the object to be updated and `**changes` are the attributes to be updated with their new values. For performance reasons, we **strongly suggest** using the ``replace`` function whenever you are transforming an `AxisArray` message and do not need its previous state. 
    
.. tip:: If, on the contrary, you would prefer a safer (but slower) implementation, you can set the environment variable ``EZMSG_DISABLE_FAST_REPLACE=1`` before running your code. It will then use the Python `dataclasses` implementation of ``replace`` with consistency checks.


Step 3: Creating the output message
=========================================================

Finally, we create the output message:

.. code-block:: python

    msg_out = replace(
        message,
        data=slice_along_axis(message.data, data_slice, axis=axis_idx),
        axes={
            **message.axes,
            downsample_axis: replace(
                axis,
                gain=axis.gain * self._state.q,
                offset=axis.offset + axis.gain * n_step,
            ),
        },
    )

.. note:: We used ``replace`` to create the output message, updating only the `data` and `axes` attributes while keeping the other attributes (like `dims`, `attrs`, and `key`) unchanged.

Step 4: Putting it all together
=========================================================

The final implementation of the ``_process`` method looks like this:

.. code-block:: python

    def _process(self, message: AxisArray) -> AxisArray:
        downsample_axis = self.settings.axis
        axis = message.get_axis(downsample_axis)
        axis_idx = message.get_axis_idx(downsample_axis)

        n_samples = message.data.shape[axis_idx]
        samples = (
            np.arange(self.state.s_idx, self.state.s_idx + n_samples) % self._state.q
        )
        if n_samples > 0:
            # Update state for next iteration.
            self._state.s_idx = samples[-1] + 1

        pub_samples = np.where(samples == 0)[0]
        if len(pub_samples) > 0:
            n_step = pub_samples[0].item()
            data_slice = pub_samples
        else:
            n_step = 0
            data_slice = slice(None, 0, None)
        msg_out = replace(
            message,
            data=slice_along_axis(message.data, data_slice, axis=axis_idx),
            axes={
                **message.axes,
                downsample_axis: replace(
                    axis,
                    gain=axis.gain * self._state.q,
                    offset=axis.offset + axis.gain * n_step,
                ),
            },
        )
        return msg_out


|ezmsg_logo_small| Final DownsampleTransformer Class
*******************************************************

Confirm that your final `DownsampleTransformer` class looks like this:

.. code-block:: python

    class DownsampleTransformer(
        BaseStatefulTransformer[DownsampleSettings, AxisArray, AxisArray, DownsampleState]
    ):
        """
        Downsampled data simply comprise every `factor`th sample.
        This should only be used following appropriate lowpass filtering.
        If your pipeline does not already have lowpass filtering then consider
        using the :obj:`Decimate` collection instead.
        """

        def _hash_message(self, message: AxisArray) -> int:
            return hash((message.axes[self.settings.axis].gain, message.key))

        def _reset_state(self, message: AxisArray) -> None:
            axis = message.get_axis(self.settings.axis)

            if self.settings.factor is not None:
                q = self.settings.factor
            elif self.settings.target_rate is None:
                q = 1
            else:
                q = int(1 / (axis.gain * self.settings.target_rate))
            if q < 1:
                ez.logger.warning(
                    f"Target rate {self.settings.target_rate} cannot be achieved with input rate of {1 / axis.gain}."
                    "Setting factor to 1."
                )
                q = 1
            self._state.q = q
            self._state.s_idx = 0

        def _process(self, message: AxisArray) -> AxisArray:
            downsample_axis = self.settings.axis
            axis = message.get_axis(downsample_axis)
            axis_idx = message.get_axis_idx(downsample_axis)

            n_samples = message.data.shape[axis_idx]
            samples = (
                np.arange(self.state.s_idx, self.state.s_idx + n_samples) % self._state.q
            )
            if n_samples > 0:
                # Update state for next iteration.
                self._state.s_idx = samples[-1] + 1

            pub_samples = np.where(samples == 0)[0]
            if len(pub_samples) > 0:
                n_step = pub_samples[0].item()
                data_slice = pub_samples
            else:
                n_step = 0
                data_slice = slice(None, 0, None)
            msg_out = replace(
                message,
                data=slice_along_axis(message.data, data_slice, axis=axis_idx),
                axes={
                    **message.axes,
                    downsample_axis: replace(
                        axis,
                        gain=axis.gain * self._state.q,
                        offset=axis.offset + axis.gain * n_step,
                    ),
                },
            )
            return msg_out


|ezmsg_logo_small| Using the DownsampleTransformer
**********************************************************

The `Downsample` class is now fully implemented and ready for use in signal processing pipelines. 
You can even use it outside of an ezmsg context by instantiating it directly and calling its ``_process`` method with an `AxisArray` message.

.. important:: The preferred way to call the ``_process`` method is to call the instance directly; below you will see that in the line: ``msg_out = downsampler(msg_in)``. This is possible because all of the processor base classes implement the ``__call__`` method, to call the ``_process`` method internally (or ``_produce`` in the case of `Producers`).

In a separate Python file in the same directory, you can test the `DownsampleTransformer` class as follows:

.. code-block:: python

    # test_downsample.py
    from downsample import DownsampleTransformer, DownsampleSettings
    import ezmsg.core as ez
    from ezmsg.util.messages.axisarray import AxisArray, LinearAxis
    import numpy as np

    # Create a DownsampleTransformer instance with desired settings.
    settings = DownsampleSettings(axis="time", target_rate=50)  # Target rate of 50 Hz.
    downsampler = DownsampleTransformer(settings)

    # Create a sample AxisArray message with a time axis and some data.
    time_axis = LinearAxis(offset=0.0, gain=0.01)  # 100 Hz sampling rate.
    data = np.random.rand(1000)  # 1000 samples of random data.
    msg_in = AxisArray(
        data=data,
        dims=["time"],
        axes={"time": time_axis},
        key="example_device",
    )

    # Process the message to downsample it.
    msg_out = downsampler(msg_in)

    print(f"Input shape: {msg_in.data.shape}, Output shape: {msg_out.data.shape}")
    print(f"Input time axis gain: {msg_in.axes['time'].gain}, Output time axis gain: {msg_out.axes['time'].gain}")

Doing the above is very handy for unit testing your processor as well as for offline processing of data.

.. note:: The `downsample` module in `ezmsg-sigproc` has a utility function for creating a `DownsampleTransformer` instance with the desired settings:

    .. code-block:: python

        def downsample(
            axis: str = "time",
            target_rate: float | None = None,
            factor: int | None = None,
        ) -> DownsampleTransformer:
            return DownsampleTransformer(
                DownsampleSettings(axis=axis, target_rate=target_rate, factor=factor)
            )

    After importing this utility function, lines 8 and 9 in our code above could now read:

    .. code-block:: python

        downsampler = downsample(axis="time", target_rate=50)

Of course, the real power of `ezmsg` comes from integrating your processor into an `ezmsg` Unit and using it in a processing pipeline. We will see how to do this next.


|ezmsg_logo_small| Creating the `Downsample ezmsg` Unit
***********************************************************

`ezmsg-sigproc` provides convenient ezmsg `Unit` wrappers for all the signal processor base classes. To do this inherit from the appropriate `ezmsg-sigproc` unit class. These are:

- `BaseProducerUnit`
- `BaseConsumerUnit`
- `BaseTransformerUnit`

The names correspond to the type of base processor class you are using. Importantly, these unit classes are agnostic to whether your processor is stateful/adaptive/async first - they will work with any of the processor base classes.

Our `Downsample` processor is a stateful transformer, so we will inherit from the `BaseTransformerUnit` class.

A lot of the behind-the-scenes work is done for you by the `BaseTransformerUnit` class, so we only need to write the following:

.. code-block:: python

    class DownsampleUnit(
        BaseTransformerUnit[DownsampleSettings, AxisArray, AxisArray, DownsampleTransformer]
    ):
        SETTINGS = DownsampleSettings


Connecting it to other `Component`\ s and initialising the transformer are accomplished in the same way that we did in the :doc:`pipeline tutorial <pipeline>`.


|ezmsg_logo_small| See Also
************************************

- `Further examples <https://github.com/iscoe/ezmsg/tree/master/examples>`_ can be found in the examples directory in `ezmsg`. These are examples of creating and using `ezmsg` Units and pipelines.
- `ezmsg-sigproc` has a large number of already implemented signal processors. More information can be found at the :doc:`ezmsg-sigproc reference <../extensions/sigproc/content-sigproc>`.
- `Downsample` class reference

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo