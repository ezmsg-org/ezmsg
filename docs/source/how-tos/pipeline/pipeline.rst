How to build an ezmsg pipeline?
#######################################

An ezmsg pipeline is built by connecting together multiple Units and Collections to form a directed graph of data processing nodes. Each Unit or Collection processes incoming messages and produces outgoing messages that can be consumed by other Units or Collections.

First, an example:

.. code-block:: python
   
   import ezmsg.core as ez
   from my_custom_module import UnitA, UnitB, CollectionC

   # Define components
   components = {
       "UNIT_A": UnitA(frequency=10),
       "UNIT_TWO": UnitB(),
       "COLLECTION_GAMMA": CollectionC(channel_count=4)
   }

   # Define connections between components
   connections = (
       (components["UNIT_A"].OUTPUT_STREAM, components["UNIT_TWO"].INPUT_STREAM),
       (components["UNIT_TWO"].OUTPUT_STREAM, components["COLLECTION_GAMMA"].INPUT_STREAM),
   )

   # Create and run the pipeline
   ez.run(components=components, connections=connections)


Now, the details. To build a pipeline, follow these steps:

- Import the ezmsg library:

    .. code-block:: python
        
        import ezmsg.core as ez

- Define the Units and Collections you want to use in your pipeline. You can use built-in Units provided by ezmsg or create your own custom Units and Collections.
- Create a dictionary of components (Units and Collections that make up your pipeline) mapping the names (as strings) you give your component instances to their corresponding objects. This dictionary should include all the Units and Collections you defined in the previous step. This is also where you instantiate your Units and Collections. For example:

    .. code-block:: python
        
        components = {
            "UNIT_A": UnitA(frequency=10),
            "UNIT_TWO": UnitB(),
            "COLLECTION_GAMMA": CollectionC(channel_count=4)
        }

- Define the connections between the components in your pipeline as a tuple of pairs. Each connection specifies which output stream of one component is connected to which input stream of another component. For example:

    .. code-block:: python
        
        connections = (
            (components["UNIT_A"].OUTPUT_STREAM, components["UNIT_TWO"].INPUT_STREAM),
            (components["UNIT_TWO"].OUTPUT_STREAM, components["COLLECTION_GAMMA"].INPUT_STREAM),
        )
- Create an ezmsg Pipeline instance by passing the components dictionary and connections tuple to the ``ezmsg.core.run()`` function, which will set up and start the pipeline. For example:

    .. code-block:: python
        
        ez.run(components=components, connections=connections)


Once the pipeline is running, the Units and Collections will process messages according to their defined behavior, and data will flow through the pipeline as specified by the connections you defined.