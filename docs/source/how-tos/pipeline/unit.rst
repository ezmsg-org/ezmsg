How to write an ezmsg Unit?
######################################

For a tutorial on this, see :doc:`../../tutorials/pipeline`. 

Here is an example of a simple ezmsg Unit:

.. code-block:: python
    
    class CountMessages(ez.Unit):
        SETTINGS = CountMessagesSettings
        STATE = CountMessagesState

        INPUT = ez.InputStream(SimpleMessage)
        OUTPUT = ez.OutputStream(EnhancedMessage)

        @ez.subscriber(INPUT)
        @ez.publisher(OUTPUT)
        async def on_message(self, message) -> AsyncGenerator:
            STATE.count += 1
            if STATE.count >= self.SETTINGS.max_count:
                raise ez.NormalTermination
            yield self.OUTPUT, EnhancedMessage(message=message, count=STATE.count)

.. important:: This example assumes that ``CountMessagesSettings``, ``CountMessagesState``, ``SimpleMessage``, and ``EnhancedMessage`` are defined elsewhere in your code. For example, 
    
    .. code-block:: python
        
        @dataclass
        class CountMessagesSettings(ez.Settings):
            max_count: int = 10

        @dataclass
        class CountMessagesState(ez.State):
            count: int = 0

        @dataclass
        class SimpleMessage:
            data: str

        @dataclass
        class EnhancedMessage:
            message: SimpleMessage
            count: int

A very simple ezmsg Unit will be made up of the following components:

- settings provided in the form of a settings class (optional, at most one)
- state provided in the form of a state class (optional, at most one)
- input / output streams (required, at least one input or output)
- method that does the processing you desire for the Unit (required, at least one method) 

As one can see from above there is a lot of flexibility in which components are used and in what quantity. In particular, one can have multiple streams and processing methods (but not settings or state) adding more functionality to each Unit. 

Our simple example above takes in a message (in the form of a SimpleMessage class) and sends out an enhanced message (the same message along with an additional count of messages received). It will stop once it reaches a predetermined `max_count` value. 

Below is an explanation of each of the components of the Unit:

|ezmsg_logo_small| Import ezmsg
***********************************************************

First, you need to import the ezmsg core module. It is convention to import it as ``ez`` for brevity.

.. code-block:: python
   
   import ezmsg.core as ez

|ezmsg_logo_small| Inherit from ezmsg.core.Unit
**********************************************************

Every ezmsg Unit must inherit from the base class ``ezmsg.core.Unit`` (``ez.Unit`` if you have imported ``ezmsg.core`` as ``ez``). This is how ezmsg recognises the class as a Unit and provides the necessary functionality to make it work within an ezmsg System.

.. code-block:: python
    
    class CountMessages(ez.Unit):
        ...


|ezmsg_logo_small| Define Settings (optional)
**************************************************
Settings provide configuration data for the Unit. They are defined as a dataclass which inherits from ``ezmsg.core.Settings`` (which is a frozen dataclass). They are typically used for configuration parameters that do not change during the execution of the Unit. In our example, our settings class contained a single variable ``max_count`` which determined how many messages the Unit would process before terminating.

.. tip:: It is good practice to name the settings class with the Unit name followed by `Settings` to make it clear which settings belong to which Unit. So, for a Unit named `CountMessages`, the settings class is named `CountMessagesSettings`.

So, if you have need of a settings class, you would define it as follows:

.. code-block:: python
    
    @dataclass
    class CountMessagesSettings(ez.Settings):
        # include settings parameters here

In the Unit itself, you will need to provide the settings class as a class attribute called ``SETTINGS``. This is how ezmsg knows what settings type to use when instantiating the Unit.

.. code-block:: python
    
    class CountMessages(ez.Unit):
        SETTINGS = CountMessagesSettings

.. note:: It is ``SETTINGS = YourSettingsClass``, not ``SETTINGS: YourSettingsClass``. This is due to how the backend initialises the Unit. We also DO NOT instantiate the settings class here (i.e., do not use ``SETTINGS = YourSettingsClass()``).

How do we actually set the settings values? **The settings attribute is initialised (with values) when the Unit is initialised**. Typically this is when you create a pipeline (see :doc:`pipeline`). If all you need to do when initialising the Unit is set the settings attribute values, then simply call the class with the desired settings values as keyword arguments. For example:

.. code-block:: python

    count_unit = CountMessages(max_count=5)

or equivalently:

.. code-block:: python

    count_unit = CountMessages(settings=CountMessagesSettings(max_count=5))

This example sets the ``max_count`` attribute in the settings for the ``CountMessages`` Unit to 5. 

Under the hood, ezmsg will create an instance of ``CountMessagesSettings`` with the provided keyword arguments and assign it to the Unit's ``SETTINGS`` attribute. It will do this automatically when you create the Unit in a pipeline by running the default implementation of the ``initialize()`` method. (see :ref:`unit_methods` for more on the ``initialize()`` method). If you want to do some custom set up logic during initialisation, you can override this method.

|ezmsg_logo_small| Define State (optional)
**************************************************
A Unit's state attribute keeps track of certain desirable variables that may change during the execution of the Unit. In our above example, we keep track of how many messages we receive. 

How we use this is very similar to the SETTINGS attribute:

- Define a state class that inherits from ``ezmsg.core.State``. This is a Python dataclass (but not frozen like ``ez.Settings``). Being a dataclass you don't need to write an ``__init__`` method; the dataclass decorator will create one for you - just define the attributes you want to keep track of.
- In the Unit, define a class attribute called ``STATE`` and assign it to the state class  you created using the syntax: ``STATE = YourStateClass`` (but do not initialise it).

That's it! Since the state is mutable, you can always set/get the state attributes using ``self.STATE.attribute_name`` within your Unit methods.

.. tip:: It is good practice to name the state class with the Unit name followed by `State` to make it clear which state belong to which Unit. So, for a Unit named `CountMessages`, the state class is named `CountMessagesState`.

Example:

.. code-block:: python
    
    @dataclass
    class CountMessagesState(ez.State):
        # include state attributes here

    class CountMessages(ez.Unit):
        STATE = CountMessagesState

|ezmsg_logo_small| Input/Output streams
**************************************************
Your unit can have any number of input and output streams. Streams are defined as class attributes using the ``ezmsg.core.InputStream`` and ``ezmsg.core.OutputStream`` classes. Each stream must be given a name (the name of the class attribute) and a message type (the type of messages that will be sent/received on that stream). Example:

.. code-block:: python
    
    class CountMessages(ez.Unit):
        INPUT = ez.InputStream(InputMessageType)
        OUTPUT = ez.OutputStream(OutputMessageType)

The ``InputMessageType`` and ``OutputMessageType`` can be any Python type:

- For something simple like keeping track of the number of messages, you would simply use ``int``. If you were passing text, then these could be ``str``.
- You can also make your own custom MessageType class. Above, I created the ``EnhancedMessage`` as a dataclass that contains a ``SimpleMessage`` attribute and integer count attribute:
    
    .. code-block:: python

       @dataclass
       class EnhancedMessage:
           msg: SimpleMessage
           count: int
- For signal processing applications, and data analysis applications, we recommend using our in-built labelled array messaging class ``AxisArray``. For more details, see :doc:`../../explanations/axisarray`.

We can use data coming in through an input stream by subscribing to it in one of our Unit methods (see :ref:`unit_methods` for more on this). Similarly, we can send data out through an output stream by publishing to it in one of our Unit methods. Finally, we need to connect the input and output streams to other Units in the pipeline (see :doc:`pipeline` for more on this).


.. _unit_methods:

|ezmsg_logo_small| Unit methods
**************************************************

You can define any number of methods in your Unit to perform the processing you desire. These can have any name you like. In our example, we defined a method called ``on_message()`` which is responsible for receiving messages from the input stream, processing them, and sending out enhanced messages through the output stream.

There are a few important notes to remember when implementing these Unit methods:

- Each method must be asynchronous to allow for non-blocking message processing. This means that each method must be defined with the ``async def`` keywords, not ``def``. 
- Each method should be decorated with the appropriate decorators (see :ref:`decorators`). The most important decorators are ``@ez.subscriber`` and ``@ez.publisher``, which are used to subscribe to input streams and publish to output streams, respectively. These two decorators should take the relevant streams as arguments.
- Each publishing method must use ``yield`` to produce output messages. The syntax of the yield statement should be ``yield self.OUTPUT_STREAM, MessageType(...)``, where ``self.OUTPUT_STREAM`` is the output stream you are publishing to, and ``MessageType(...)`` is the message you are sending. 
- Each subscribing method must take in a message parameter which will receive the incoming message from the input stream. The method signature should be ``async def method_name(self, message)``. Additionally, in the ``ez.subscriber`` decorator, you can specify the keyword boolean argument ``zero_copy`` to indicate whether you want to receive a zero-copy reference (``zero_copy=True``) to the message (if supported by the message type) or a copy of the message. The default is ``zero_copy=False``.
- If a method is to stop processing and terminate normally, it should raise the ``ez.NormalTermination`` exception. This indicates to ezmsg that the Unit has completed its task and can be safely terminated.
- There are other decorators available for other purposes. See :ref:`decorators` for more details. Note, one can stack decorators. 

With these components discussed, we can see the example from this question again:

.. code-block:: python
    
    class CountMessages(ez.Unit):
        SETTINGS = CountMessagesSettings
        STATE = CountMessagesState

        INPUT = ez.InputStream(SimpleMessage)
        OUTPUT = ez.OutputStream(EnhancedMessage)

        @ez.subscriber(INPUT)
        @ez.publisher(OUTPUT)
        async def on_message(self, message) -> AsyncGenerator:
            STATE.count += 1
            if STATE.count >= self.SETTINGS.max_count:
                raise ez.NormalTermination
            yield self.OUTPUT, EnhancedMessage(message=message, count=STATE.count)

The asynchronous ``on_message()`` method is decorated with both the ``@ez.subscriber(INPUT)`` and ``@ez.publisher(OUTPUT)`` decorators. This indicates that the method subscribes to the ``INPUT`` stream and publishes to the ``OUTPUT`` stream. The method takes in a ``message`` parameter, which will receive messages of type ``SimpleMessage`` from the ``INPUT`` stream. Inside the method, we increment the ``count`` attribute in the ``STATE`` by 1 each time a message is received. If the count reaches the ``max_count`` value from the ``SETTINGS``, we raise ``ez.NormalTermination`` to indicate that the Unit has completed its task. Otherwise, we yield an ``EnhancedMessage`` containing the original message and the current count to the ``OUTPUT`` stream.

|ezmsg_logo_small| The Unit backend
**************************************************

When a Unit is initialised within a pipeline, ezmsg takes care of setting up the necessary infrastructure to manage the Unit's execution. This includes:

- Initialising the settings and state attributes based on the provided classes by running the ``initialize()`` method.
- Setting up the input and output streams to facilitate message passing between Units. Each publishing stream comes with message channels for each process containing subscribers connected to it. This manages transport via local cache, sharedmemory or TCP depending on location of the relevant subscriber.
- Registering the Unit methods with the appropriate decorators to handle message processing. The ``@ez.subscriber`` and ``@ez.publisher`` decorators, are registered for message transport. Other decorators like ``@ez.main``, or ``@ez.task`` define which process the method runs in (the Unit's main process, or a separate task process, respectively).



.. |ezmsg_logo_small| image:: ../../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo