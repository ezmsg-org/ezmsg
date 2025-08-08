How to run ezmsg in a Jupyter notebook?
####################################################################


If you have an ezmsg pipeline defined in a Jupyter notebook, you can run it directly from the notebook as long as you call the `ez.run()` function with the optional keyword argument ``force_single_process=True``:

   .. code-block:: python

      ez.run(components=components, connections=connections, force_single_process=True)

Here the components are the units of your pipeline and the connections define how the components are connected (for more details, see :ref:`creating-pipeline`).

.. |ezmsg_logo_small| image:: ../../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
