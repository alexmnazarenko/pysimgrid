.. _simdag_algorithms:

***************************
pysimgrid.simdag.algorithms
***************************

Package containing some pre-made scheduling algorithms for benchmarking.

.. contents::

Dynamic scheduling
==================

Dynamic algorithms work along the simulation and schedule only a subset of tasks on each execution (most commonly the tasks that are
ready for execution right now).

.. autosummary::
  :nosignatures:

  pysimgrid.simdag.algorithms.OLB
  pysimgrid.simdag.algorithms.MCT
  pysimgrid.simdag.algorithms.BatchMin
  pysimgrid.simdag.algorithms.BatchMax
  pysimgrid.simdag.algorithms.BatchSufferage


.. autoclass:: pysimgrid.simdag.algorithms.OLB

.. autoclass:: pysimgrid.simdag.algorithms.MCT

.. autoclass:: pysimgrid.simdag.algorithms.BatchMin

.. autoclass:: pysimgrid.simdag.algorithms.BatchMax

.. autoclass:: pysimgrid.simdag.algorithms.BatchSufferage

Static scheduling
=================

Static algorithms are executed before the simulation and schedule all tasks at once.

.. autosummary::
  :nosignatures:

  pysimgrid.simdag.algorithms.HCPT
  pysimgrid.simdag.algorithms.HEFT
  pysimgrid.simdag.algorithms.Lookahead
  pysimgrid.simdag.algorithms.PEFT
  pysimgrid.simdag.algorithms.RandomStatic
  pysimgrid.simdag.algorithms.RoundRobinStatic


.. autoclass:: pysimgrid.simdag.algorithms.HCPT

.. autoclass:: pysimgrid.simdag.algorithms.HEFT

.. autoclass:: pysimgrid.simdag.algorithms.Lookahead

.. autoclass:: pysimgrid.simdag.algorithms.PEFT

.. autoclass:: pysimgrid.simdag.algorithms.RandomStatic

.. autoclass:: pysimgrid.simdag.algorithms.RoundRobinStatic
