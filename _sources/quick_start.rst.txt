***********
Quick start
***********

.. contents::

Introduction
============

As SimGrid itself, pysimgrid is all about simulations in distributed systems.

SimGrid supports different kinds of workloads, but for now only directed acyclic graph (DAG) workload is supported.

To run the simulation, 3 things are required:

* Platform definition describing a network of hosts.

  Platforms are defined as xml files with a specific scheme.
  Some platforms can be found in tests and examples of pysimgrid and Simgrid itself.

  More details about platforms can be found in
  `SimGrid documentation <http://simgrid.gforge.inria.fr/simgrid/latest/doc/platform.html>`_.

* DAG (workload) description.

  DAGs can be defined in one of 2 formats: dax and dot.

  Nodes of DAG represent a computational tasks, while edges represent data dependencies (communication tasks).

  Each computational task has an associated computational cost in flops.

  Each communication task has an associated transfer amount in bytes.

* Scheduling algorithm that maps workload to a platform.

  Multiple scheduling algorithms are implemented in pysimgrid (see :ref:`simdag_algorithms`).

Running a simulation from python
================================

Simplest way to use pysimgrid is to run a simulation from your own script.

It is quite easy, but you must mind SimGrid limitation - only one simulation can be run in a process.
To see some basic output from the simulation, don't forget to configure the logging module::

    import logging
    import pysimgrid.simdag
    import pysimgrid.simdag.algorithms

    logging.basicConfig(level=logging.DEBUG, format=_LOG_FORMAT, datefmt=_LOG_DATE_FORMAT)

    with pysimgrid.simdag.Simulation(platform, "task_file.dot") as simulation:
      scheduler = pysimgrid.simdag.algorithms.HEFT(simulation)
      scheduler.run()

If you want to run multiple simulations in a single script, easiest way to do so is to employ the multiprocessing module::

    import multiprocessing
    import pysimgrid.simdag
    import pysimgrid.simdag.algorithms

    def run_simulation(platform):
      with pysimgrid.simdag.Simulation(platform, "task_file.dot") as simulation:
        scheduler = pysimgrid.simdag.algorithms.HEFT(simulation)
        scheduler.run()
        return simulation.clock

    ctx = multiprocessing.get_context("spawn")
    platforms = ["platform1.xml", "platform2.xml"]
    with ctx.Pool(processes=NO_PARALLEL, maxtasksperchild=1) as pool:
      results = pool.map(run_simulation, platforms)


Running a batch simulation from shell
=====================================

Single simulations won't get you far in algorithm benchmarking.

Of course you can setup batch execution by yourself, but there is a bundled tool for that - :ref:`pysimgrid.tools.experiment`.

It makes running a publication-worthy experiments as easy as::

    python -m pysimgrid.tools.experiment platforms_dir workflows_dir algorithms.json output.json -j8

As you can see, batch simulation setup requires 3 main inputs:

* List of simulated platforms (provided as a directory with *.xml* platform definitions).
  See `SimGrid platform documentation <http://simgrid.gforge.inria.fr/simgrid/latest/doc/platform.html>`_ for more details.

* List of workflows that will be scheduled (provided as directory with \*.dot or \*.dax workflows).

  * A lot of DAX examples can be found in
    `Pegasus Workflow Generator <https://confluence.pegasus.isi.edu/display/pegasus/WorkflowGenerator>`_.;

  * Some .dot examples can be found in the SimGrid sources;

  * You can generate random .dot workflows using a provided tool pysimgrid.tools.dag_gen.

* List of scheduling algorithms (provided as a json file)

The batch run will make a cartesian product of all three lists thus running N_platforms x N_workflows x N_algorithms simulations.

Algorithms json example::

    [
      {
        "class": "pysimgrid.simdag.algorithms.HCPT",
        "name": "HCPT"
      },
      {
        "class": "pysimgrid.simdag.algorithms.HEFT",
        "name": "HEFT"
      }
    ]

Easiest way to run pysimgrid.tools.experiment module is to use *python -m* syntax. The tools has some configuration options, but the most important one is
probably *-j* that allows to configure number of parallel simulations to run. To see more check the tool help, like this::

    python -m pysimgrid.tools.experiment --help

The result of simulation is a json file that is easy to analyse using python. Output format is::

    [
      {
      "platform": "platform_dir/cluster_10_1-4_100_100_61.xml"
      "tasks": "workflows_dir/testg0.4.dot",
      "algorithm": {"name": "HEFT", "class": "pysimgrid.simdag.algorithms.heft.HEFTScheduler"},
      "makespan": 1109.8607620759176,
      "exec_time": 6228.565137126975,
      "comm_time": 890.1153923229831,
      "expected_makespan": 1039.0626220703125,
      "sched_time": 0.08849978446960449,
      },
      ...
      ...
    ]

Let's decipher it field by field:

* platform - platform definition file

* tasks - workflow definition file

* algorithm - algorithm name and python class

* makespan - measured makespan of the workflow (simulation clock)

* exec_time - total time spend on computation (simulation clock)

* comm_time - total time spent on communication (simulation clock)

* expected_makespan - makespan as expected by static scheduling algorithm
  (simulation clock, may be NaN if algorithm doesn't provide this info)

* sched_time - real time spent by scheduler (wall clock)

Writing your own scheduler
==========================

The very reason for pysimgrid to exist is to make custom scheduler implementation as simple as possible.

To begin, you need to select the scheduler type:

* Static - all scheduling decisions are made before the simulation
* Dynamic - scheduling is performed as simulation runs

Then you should inherit from a proper base class and implement its interface.
The actual SimGrid API you will need is pretty narrow and self-explained, so it can be deciphered straight from code examples.

Dynamic scheduler
-----------------

Let's begin with a simplest dynamic scheduler that just schedules ready tasks to the fastest free host.
Note that it should be ensured manually, as SimGrid will happily accept multiple active tasks on a same host (it will simply divide the computing
power equally)::

    from pysimgrid import simdag

    class SimpleDynamic(simdag.DynamicScheduler):
      def prepare(self, simulation):
        for h in simulation.hosts:
          h.data = {}

      def schedule(self, simulation, changed):
        for h in simulation.hosts:
          h.data["free"] = True
        for task in simulation.tasks[(simdag.TaskState.TASK_STATE_RUNNING,
                                      simdag.TaskState.TASK_STATE_SCHEDULED)]:
          task.hosts[0].data["free"] = False
        for t in simulation.tasks[simdag.TaskState.TASK_STATE_SCHEDULABLE]:
          free_hosts = simulation.hosts.by_data("free", True).sorted(lambda h: t.get_eet(h))
          if free_hosts:
            t.schedule(free_hosts[0])
            free_hosts[0].data["free"] = False
          else:
            break

Static scheduler
----------------

Usually, the idea behind static scheduling is to take into account the full structure of the workflow. However, for the simple example
let's focus on a static scheduler interface::

    import random
    import networkx
    from pysimgrid import simdag

    class RandomSchedule(simdag.StaticScheduler):
      def get_schedule(self, simulation):
        schedule = {host: [] for host in simulation.hosts}
        graph = simulation.get_task_graph()
        for task in networkx.topological_sort(graph):
          schedule[random.choice(simulation.hosts)].append(task)
        return schedule

There are few things to note there:

* StaticScheduler is expected to return a full schedule as dict {host: [list_of_tasks...]}
* Naturally, this format allows to produce 'impossible' schedules and it is up to scheduler to ensure that no child task precedes its parent.
  In this example it is achieved by networkx.topological_sort.

Meaningful static scheduling is much more complicated, as you'll require some platform model to 'predict' task and host states in future.
This platform model is typically much simpler than SimGrid and disregards network topology and saturation.

Realistic examples of static schedulers can be found in :ref:`simdag_algorithms` package.
