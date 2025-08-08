How to run ezmsg from a python script?
####################################################################


If you have an ezmsg pipeline defined in a python script, you can run it in two ways:

1. (sandbox mode) Directly from the script using the python interpreter. 
2. (client-server mode) Using the command line interface to connect to an ezmsg instance running in the background. 

The latter is explained in :doc:`commandline`. 

The former is explained below. 

.. note:: We call running ezmsg without using the command line interface "*sandbox mode*". This is because ezmsg will automatically set up a local `GraphServer` to manage the pipeline execution. It will automatically choose appropriate settings for the `GraphServer`, including the address (host and port). In order to connect to this `GraphServer`, you would need to guess the address set by ezmsg. Strictly speaking, this is not a true sandbox, but this nomenclature is used to indicate that we are not setting any `GraphServer` details and we are not intending to manage the `GraphServer`. 
    
.. note:: If you want to specify the address of the `GraphServer`, you can either set the ``EZMSG_GRAPH_SERVER`` environment variable before running your script or start an ezmsg instance from the command line with the `serve` or `start` commands, which will allow you to specify the address and port. This is explained in :doc:`commandline`


|ezmsg_logo_small| The ``run()`` function
***********************************************************

In order to run the ezmsg pipeline in any mode, your script must contain a call to ``ezmsg.core.run()``. You can consult the API reference for :doc:`run() here <../../reference/API/entrypoint>`.

At minimum, you need to provide the components and connections of your pipeline to the ``run()`` function.

.. note:: Generally we suggest importing ezmsg as ``import ezmsg.core as ez`` and then using the shorthand ``ez.run()``. This is just a convention, but it makes the code easier to read and understand.

As an example, consider the example script provided in :doc:`../../tutorials/run`. 

.. tip:: In order to be able to use your script as a script and module, it is good practice to wrap the call to ``ez.run()`` in a ``if __name__ == "__main__":`` block. This ensures that the pipeline is only run when the script is executed directly, and not when it is imported as a module in another script.


|ezmsg_logo_small| Run the ezmsg pipeline from your script
***********************************************************

To run the pipeline, you can use the following command in the :term:`terminal`:

.. code-block:: bash

   python3 tutorial_pipeline.py

This will start the pipeline and begin processing data. 


.. |ezmsg_logo_small| image:: ../../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
