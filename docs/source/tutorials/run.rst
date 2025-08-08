3. Running An Ezmsg System
############################

**************************
Build a pipeline
**************************

In order to run an ezmsg system, we need a pipeline. A pipeline is a collection of units that process data in a specific order. You can build a pipeline by defining units and connecting them together. See the `:doc:`tutorials/pipeline` for more information on how to build a pipeline.


***********************************
Run the pipeline using your script
***********************************

Note, this sets graphserver address (host and port) to the default values. If you want to use a different address, you can set the `EZMSG_GRAPH_SERVER` environment variable before running your script.

To run the pipeline, you can use the following command:
.. code-block:: bash

   python your_script.py    

This will start the pipeline and begin processing data. Make sure to replace `your_script.py` with the name of your script that contains the pipeline definition.

.. note::An ezmsg pipeline can be run in a notebook environment with an extra parameter. When using ``run()``, include the optional kwarg ``force_single_process=True``.

*****************************************
Run the pipeline using the command line
*****************************************

=================================
Start an ezmsg system
=================================

To start an ezmsg system, you can use the ``ezmsg`` command-line interface. Here is an example command:
.. code-block:: bash

   ezmsg serve --address localhost:5000

This command will start the ezmsg system and listen for incoming connections on `localhost` at port `5000`. You can change the address and port as needed.

You can also use the ``start`` command to create a new ezmsg system but fork the current process. This is useful for running ezmsg in the background or as a service:
.. code-block:: bash

   ezmsg start --address localhost:5000

==========================
Run a pipeline in ezmsg
==========================

Now that an ezmsg server is running one can run a pipeline in it, by running your pipeline script from a separate terminal window. 


.. code-block:: bash    

   python your_pipeline.py


If you're working with the ezmsg repository, you can ensure you're running with expected dependency versions by using `uv run`:
.. code-block:: bash

   uv run ezmsg start your_pipeline.py

.. note: Run the script from a separate terminal window to keep the ezmsg server running in the background.

===========================
Shutdown the ezmsg system
===========================

***********************************
Miscellaneous command line options
***********************************

Other than the `serve`, `start` and `shutdown` commands, the ezmsg command-line interface allows you to visualise your pipeline graphically. 

the two available commands are `graphviz` and `mermaid`. 

===========================
Mermaid visualisation
===========================

To visualise your pipeline using Mermaid, you can use the following command:
.. code-block:: bash

   ezmsg mermaid your_pipeline.py

This will generate a Mermaid diagram of your pipeline and display it in the terminal. You can then copy and paste the diagram into a Mermaid live editor to view it graphically.

include picture here

===========================
Graphviz visualisation
===========================

To visualise your pipeline using Graphviz, you can use the following command:
.. code-block:: bash

   ezmsg graphviz your_pipeline.py

This will generate a Graphviz diagram of your pipeline and display it in the terminal. You can then copy and paste the diagram into a Graphviz live editor to view it graphically.

==============================================
Compactify the pipeline graph visualisation
==============================================

To compactify the pipeline graph visualisation, you can use the following command:
.. code-block:: bash

   ezmsg mermaid your_pipeline.py --compact

or 

.. code-block:: bash

   ezmsg mermaid your_pipeline.py -c

Note this option is stackable if your pipeline is very deep (collections within collections)

include picture here