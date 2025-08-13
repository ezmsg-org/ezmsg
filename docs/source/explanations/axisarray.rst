About AxisArray
######################

(under construction)
``AxisArray`` is a specialized message format used within the ``ezmsg`` framework to represent multi-dimensional data structures. It is designed to handle complex data types efficiently, making it suitable for applications in signal processing, scientific computing, and data analysis.

Inspiration for the design of ``AxisArray`` comes from various sources, including:

- The need for a flexible and efficient way to represent multi-dimensional data in a streaming context.
- The desire to create a unified data format that can be easily manipulated and processed within the ``ezmsg`` framework.
- The goal of providing a high-level abstraction for working with complex data types, making it easier for users to develop signal processing applications.

(include list here)


****************************
Description
****************************

providing a flexible and extensible framework for signal processing tasks makes it 
- easier for users to create custom signal processors
- easier for users to integrate with ezmsg and create ezmsg Units 
- easier to create processing pipelines in the ``ezmsg`` ecosystem
- allows standalone use outside of an ezmsg context

**********************************************
Recommended Use
**********************************************

=======================
When to use AxisArray
=======================

When the data is in array form, requires labels, 

Anything that is inherently multi-dimensional and needs to be processed in a multi-dimensional context, ``AxisArray`` is the preferred message format. It is particularly useful for applications in signal processing, scientific computing, and data analysis where complex data structures are common.

Can also be used as a trigger for changing state or settings by sending specific terms in the ``attr`` field.

Not necessary if sending simple data types like integers, floats, or strings. In such cases, using the python formats is more efficient and likely easier to use.

==========================
methods
==========================

there are many built in methods for manipulating AxisArray objects, such as:
(replace with actual methods from AxisArray class)
- `replace`: Modify the data within the AxisArray.
- `filter`: Apply a filter to the data.
- `transform`: Apply a transformation to the data.
- `aggregate`: Combine data from multiple AxisArray objects.



=================================
How to return an AxisArray object
=================================
To return an ``AxisArray`` object, you can create an instance of the ``AxisArray`` class and populate it with your data. Here is a simple example:

use `replace` to modify the data as needed before returning the ``AxisArray`` object.

********************************
See Also
********************************

1. `Ezmsg Documentation <ezmsg_documentation>`_
#. :doc: `axisarray`
#. :doc: `sigproc`
#. :doc: `../tutorials/start`
#. :doc: `../how-tos/basics/main`
#. :doc: `../how-tos/pipeline/main`
