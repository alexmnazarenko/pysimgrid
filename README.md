DAG simulation tool
===================

* Simulate different scheduling schemes using SimGrid framework.
* Visualize simulations as Gantt charts

Subprojects:
* Simulator itself (C++, project 'simulate')
* Visualize module (Python, project 'visualize')
* Batch run module (Python, project 'runner')


Examples
========

TODO


Dependencies
============

C++

* C++11 capable compiler (tested with GCC/G++ 4.8.4)
* CMake 3.2+
* SimGrid itself built with graphviz support (tested v3.12) [SimGrid download page](http://simgrid.gforge.inria.fr/download.php)
* RapidJSON (bundled for now, v1.0.2)
* Boost libraries: program_options, filesystem, system (tested v1.54)

Python

* python 3.4+
* python-gantt [https://bitbucket.org/xael/python-gantt]


Build instructions
==================

Ubuntu 14.04+
-------------

TODO


FAQ
===

1. Where to get platform definition files? They look scary.
-----------------------------------------------------------

SimGrid source distribution contains quite a few of platform examples of different complexity and scale.


2. Where to get some ready DAGs?
--------------------------------

* Pegasus workflows, DAX format  
    * See [Pegasus Workfow Generator](https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator)
    * Direct download (warning: ~400Mb) [[link](https://download.pegasus.isi.edu/misc/SyntheticWorkflows.tar.gz)]


3. What about multi-core tasks?
-------------------------------

They are not supported for now. Two main reasons:

* Existing SimGrid task parsers do not support them, so you need custom task format to even setup this.
* There are some [discouraging words](http://simgrid.gforge.inria.fr/simgrid/3.12/doc/platform.html#pf_Cr) in SimGrid documentation about validity of such simulation.
