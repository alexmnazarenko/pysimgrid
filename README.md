pysimgrid
=========

The main goal of the project is to create an easy way to implement and benchmark
scheduling algorithms while leveraging the powerful [SimGrid](http://simgrid.gforge.inria.fr) simulation framework.

#### Features:

* Simulate different scheduling distributed platforms
* Implement your own scheduling algorithms
* Compare them with well-known approaches (e.g. HEFT). Batteries included )
* Run your simulations in parallel

#### Documentation:

https://alexmnazarenko.github.io/pysimgrid/index.html

Examples
========

Running a simulation:

```python
from pysimgrid import simdag
import pysimgrid.simdag.algorithms as algorithms

with simdag.Simulation("test/data/pl_4hosts.xml", "test/data/basic_graph.dot") as simulation:
  scheduler = algorithms.Lookahead(simulation)
  scheduler.run()
  print(simulation.clock, scheduler.scheduler_time, scheduler.total_time)
```


Implementing your very own scheduling algorithm:

```python
from pysimgrid import simdag
import networkx

class RandomSchedule(simdag.StaticScheduler):
  def get_schedule(self, simulation):
    schedule = {host: [] for host in simulation.hosts}
    graph = simulation.get_task_graph()
    for task in networkx.topological_sort(graph):
      schedule[random.choice(simulation.hosts)].append(task)
    return schedule
```


Dependencies
============

C++

* C++11 capable compiler (tested with GCC/G++ 4.8.4)
* [SimGrid](http://simgrid.gforge.inria.fr/download.php) built with graphviz support (build script provided, tested v3.13)
* [CMake](https://cmake.org/) (for building SimGrid)

Python

* python 2.7 or 3.4+
* [setuptools](https://pypi.python.org/pypi/setuptools)
* [Cython](http://cython.org/)
* [numpy](http://www.numpy.org/)
* [networkx](https://networkx.github.io/)



Build instructions
==================

Ubuntu 14.04+
-------------

Install system dependencies (list is not full):

```bash
sudo apt-get install libboost-context-dev libboost-program-options-dev libboost-filesystem-dev doxygen graphviz-dev
```

Use provided scripts to get dependencies:

```bash
./get_simgrid.sh
```

Installation:

```bash
python3 setup.py install --user
```

Development build
-----------------

Inplace build

```bash
python3 setup.py build_ext --inplace
```

Test the build:

```bash
python3 run_tests.py
```

FAQ
===

#### 1. Where to get platform definition files? They look scary.

SimGrid source distribution contains quite a few of platform examples of different complexity and scale.


#### 2. Where to get some ready DAGs?

* Pegasus workflows, DAX format  
    * See [Pegasus Workfow Generator](https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator)
    * Direct download (warning: ~400Mb) [[link](https://download.pegasus.isi.edu/misc/SyntheticWorkflows.tar.gz)]


#### 3. What about multi-core tasks?

They are not supported for now. Two main reasons:

* Existing SimGrid task parsers do not support them, so you need custom task format to even setup this.
* There are some [discouraging words](http://simgrid.gforge.inria.fr/simgrid/latest/doc/platform.html#pf_Cr)
in SimGrid documentation about validity of such simulation.
* API seems to be "in progress"
