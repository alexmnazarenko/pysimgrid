
pysimgrid
=========

.. only:: html

    :Release: |version|
    :Date: |today|


pysimgrid is a framework aimed mainly to simplify DAG scheduling algorithm development, testing and benchmarking.
It is based on an awesome `SimGrid library <http://simgrid.gforge.inria.fr/>`_ that provides a reliable simulation core complemented by python's
ease of development and wide array of libraries (import antigravity, anyone?).

Current features:

* Simulation of DAG execution on a distributed network
* Array of popular scheduling algorithms (see :ref:`simdag_algorithms`)
* Convenience tools for experiments

  * Batch/parallel simulations
  * Platform generator
  * DAG generator
  * BoT generator


Documentation
=============


.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quick_start
   reference/index
