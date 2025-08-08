How to create an ezmsg Collection of Units?
###############################################

We can optionally combine several Units into a single node called a Collection. 

From an API perspective, a Collection is very similar to a Unit. It can have its own settings and state, and it can have input and output streams. For details on how to write an ezmsg Unit, see :doc:`unit`. However, instead of processing messages directly, a Collection contains multiple Units and defines how they are connected together.

First, define the member Units. Then use ``configure()`` to apply settings to the component Units at initialisation. Finally use ``network()`` to provide an iterable of pairs that define how the Units are connected. Remember, connections are directed (one-way), so the pairs are in the form ``(from-Unit, to-Unit)``. As an example, in the first connection pair provided below, the first element is the output stream of ``COUNT``, and the second element is the input stream of ``ADD_ONE``.

.. code-block:: python
    
    class CountCollection(ez.Collection):

        # Define member units
        COUNT = Count()
        ADD_ONE = AddOne()
        PRINT = PrintValue()

        # Use the configure function to apply settings to member Units
        def configure(self) -> None:
            self.COUNT.apply_settings(CountSettings(iterations=20))

        # Use the network function to connect inputs and outputs of Units
        def network(self) -> ez.NetworkDefinition:
            return (
                (self.COUNT.OUTPUT_COUNT, self.ADD_ONE.INPUT_COUNT),
                (self.ADD_ONE.OUTPUT_PLUS_ONE, self.PRINT.INPUT)
            )

Our example does not contain any settings or state for the Collection itself, but you can add these as needed. For example, you might want to add a setting that is common to multiple member Units, and then apply that setting to each member Unit in the ``configure()`` method.

.. note:: Within the ``configure()`` method, we applied settings to the ``Count`` Unit by using an in-built method called ``apply_settings()``. This method takes an instance of the settings class and applies it to the Unit. In this case, we set the number of iterations to 20, meaning that the Count Unit will generate numbers from 0 to 19.

.. note:: The ``configure()`` method is optional. If you do not need to apply settings to the member Units, you can omit it. The ``network()`` method is also optional, but it is required if you want to connect the Units together in a specific way. If you do not provide a ``network()`` method, the Units will not be connected and will not communicate with each other.