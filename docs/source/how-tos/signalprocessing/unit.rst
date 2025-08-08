How to turn a signal processor into an ``ezmsg`` Unit?
#######################################################

To convert a signal processor to an ``ezmsg`` Unit, you can follow these steps:

1. **Define the Processor**: Create a class that inherits from the appropriate signal processor template (e.g., `SignalProcessor`, `Filter`, etc.).
2. **Implement the Processing Logic**: Override the necessary methods to implement the signal processing logic.
3. **Define Input and Output Ports**: Use the `ezmsg` port system to define input and output ports for the signal processor.
4. **Register the Unit**: Use the `ezmsg` registration system to register the signal processor as an `ezmsg` Unit.

(under construction)