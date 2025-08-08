List of Extensions
#######################

`ezmsg` extensions can be installed individually or all at once. To install all the extension packages in one go, you can use the following command:

.. code-block:: bash

    pip install "ezmsg[all_ext]"


This will install all the available public extension packages for `ezmsg` that are listed in `pyproject.toml`.
If you prefer to install a subset of extension packages, you can use the following command:

.. code-block:: bash

    pip install "ezmsg[zmq,sigproc,...]"

Please note that the `ezmsg` package itself can still be installed without any additional extensions using `pip install ezmsg`.

Extensions can be managed manually as well. Here are some of the extensions we manage or are aware of:

- `ezmsg-sigproc <https://github.com/ezmsg-org/ezmsg-sigproc>`_ -- Timeseries signal processing modules
- `ezmsg-websocket <https://github.com/ezmsg-org/ezmsg-websocket>`_ -- Websocket server and client nodes for `ezmsg` graphs
- `ezmsg-zmq <https://github.com/ezmsg-org/ezmsg-zmq>`_ -- ZeroMQ pub and sub nodes for `ezmsg` graphs
- `ezmsg-panel <https://github.com/griffinmilsap/ezmsg-panel>`_ -- Plotting tools for `ezmsg` that use `panel <https://github.com/holoviz/panel>`_
- `ezmsg-blackrock <https://github.com/griffinmilsap/ezmsg-blackrock>`_ -- Interface for Blackrock Cerebus ecosystem (incl. Neuroport) using `pycbsdk`
- `ezmsg-lsl <https://github.com/ezmsg-org/ezmsg-lsl>`_ -- Source unit for LSL Inlet and sink unit for LSL Outlet
- `ezmsg-unicorn <https://github.com/griffinmilsap/ezmsg-unicorn>`_ -- g.tec Unicorn Hybrid Black integration for `ezmsg`
- `ezmsg-gadget <https://github.com/griffinmilsap/ezmsg-gadget>`_ -- USB-gadget with HID control integration for Raspberry Pi (Zero/W/2W, 4, CM4)
- `ezmsg-openbci <https://github.com/griffinmilsap/ezmsg-openbci>`_ -- OpenBCI Cyton serial interface for `ezmsg`
- `ezmsg-ssvep <https://github.com/griffinmilsap/ezmsg-ssvep>`_ -- Tools for running SSVEP experiments with `ezmsg`
- `ezmsg-vispy <https://github.com/pperanich/ezmsg-vispy>`_ -- `ezmsg` visualization toolkit using PyQt6 and vispy.

|ezmsg_logo_small| Extension API References
***********************************************

Further details are available for the following extensions:

.. toctree::
   :maxdepth: 2

   sigproc/content-sigproc

.. |ezmsg_logo_small| image:: ../_static/_images/ezmsg_logo.png
  :width: 40
  :alt: ezmsg logo
