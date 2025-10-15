How to run ezmsg from the command line?
##########################################

In order to run an ezmsg pipeline from the command line, you need a python script with the following three things:

1. a dictionary of components (Units or Collections)
2. a tuple of connections between (the input and output streams of) the components
3. a call to ``ezmsg.core.run()`` with the components and connections as arguments.

The ezmsg pipeline is a graph defined by these components and connections. The ``run()`` function is the entry point to start the pipeline.  

When we use the command line to run an ezmsg pipeline, we can choose the address of the backend server (`GraphServer`) that will manage the pipeline execution. This is useful when we want to run multiple pipelines on the same machine or when we want to run a pipeline on a remote machine.


|ezmsg_logo_small| Run the pipeline using the command line
************************************************************

The ``ezmsg`` command line interface exposes extra tools to manage a pipeline that is running in the background on a machine. Run ``ezmsg -h`` to see all the available options. Currently, there are five commands available: ``start``, ``serve``, ``shutdown``, ``mermaid``, and ``graphviz``. The first three are used to manage the ezmsg system, while the last two are used for visualising the pipeline.


Step 1. Start the ezmsg system
=================================

Paste and run the following in your terminal:

.. code-block:: bash

   ezmsg serve --address 127.0.0.1:25978

This command will start the ezmsg system and listen for incoming connections on host `127.0.0.1` at port `25978`. You can change the address and port as needed - the values here are actually the default that `ezmsg` will use if you leave out the ``--address`` option.

.. note:: You will notice that with the command ``serve``, ezmsg takes over your terminal in the sense that it runs ezmsg in the foreground, not allowing you to use this terminal for other purposes. In order to continue using the terminal, you will need to open a new terminal (you can have multiple terminals open). If instead you use the command ``start``, ezmsg will be forked to another process allowing you to use the current terminal. This is useful for running ezmsg in the background or as a service:

   .. code-block:: bash

      ezmsg start --address 127.0.0.1:25978


Step 2. Run the pipeline in ezmsg
==================================

Now that an ezmsg instance is running one can connect and run a pipeline to it. 
You will need to change the way you call ``ez.run()`` by adding an address to the call. 

In your script, change the ``run()`` call to include the address from Step 1. For example:

.. code-block:: python

   ez.run(components=components, connections=connections, address="127.0.0.1:25978")

Now you can run your pipeline script from a **new** terminal window.

.. code-block:: bash    

   python3 my_pipeline.py


Step 3. Shutdown ezmsg instance
=================================

When you are done with ezmsg, you can shutdown the ezmsg system by running the following command in a new terminal window:

.. code-block:: bash

   ezmsg shutdown --address 127.0.0.1:25978

This will gracefully shutdown the ezmsg system and release any resources it was using. Make sure to use the same address you used to start and run the ezmsg system. 

.. note:: If you used the ``serve`` command to start the ezmsg system, after running the ``shutdown`` command, you will see that the terminal where you started the ezmsg system will return to the command prompt. 

|ezmsg_logo_small| What happens under the hood?
************************************************************

What actually happens when you run a script with an ezmsg pipeline and a ``run()`` function is that ezmsg starts a backend server called a **`GraphServer`**. This `GraphServer` manages the operation of any ezmsg pipeline connected to it. The naming comes from the fact that ezmsg pipelines are simply nodes - Units or Collections - connected in a :term:`directed acyclic graph` (:term:`DAG`\ ). 

The `GraphServer` is coupled with a **`SharedMemoryServer`** that allows for efficient data transfer between the different components of the pipeline. This is particularly useful for high-throughput applications where low latency is crucial. It utilises the in-built Python ``multiprocessing.shared_memory`` library to handle the data transfer between processes.

The reverse is true too: you can run ezmsg instance (start a `GraphServer`) without a pipeline, but until you attach an ezmsg pipeline nothing interesting will happen.


.. |ezmsg_logo_small| image:: ../../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
