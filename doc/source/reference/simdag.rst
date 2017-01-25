****************
pysimgrid.simdag
****************

.. contents::

Simulation
==========

Simulation class provides an entry point for the, ugh, simulations.

It is intended to be used as context manager, as it is important to ensure proper setup and cleanup
when working with a native library.

.. autoclass:: pysimgrid.simdag.Simulation
   :members:

Schedulers
==========

Schedulers are the main workhorses during the simulation.

While nothing stops you from doing everything manually, scheduler interfaces
provides a useful framework to build your algorithm upon.


.. autoclass:: pysimgrid.simdag.Scheduler
  :members:

.. autoclass:: pysimgrid.simdag.StaticScheduler
  :show-inheritance:

  .. automethod:: pysimgrid.simdag.StaticScheduler.get_schedule

.. autoclass:: pysimgrid.simdag.DynamicScheduler
  :show-inheritance:

  .. automethod:: pysimgrid.simdag.DynamicScheduler.prepare

  .. automethod:: pysimgrid.simdag.DynamicScheduler.schedule


Task kinds
==========

SimGrid tasks can have different types.

Parallel tasks API is unfinished for now, there a only 2 important ones:
  *TASK_KIND_COMP_SEQ* - computational task

  *TASK_KIND_COMM_E2E* - data transfer task (dependency)

:class:`pysimgrid.simdag.Simulation` class provides shortcut methods to access tasks by kind::

    import pysimgrid.simdag

    with pysimgrid.simdag.Simulation(platform, "task_file.dot") as simulation:
      # get all computational tasks
      tasks = simulation.tasks
      # get all computational tasks "manually"
      tasks = simulation.all_tasks[pysimgrid.simdag.TaskKind.TASK_KIND_COMP_SEQ]
      # get all connections
      connections = simulation.connections
      # get all connections "manually"
      connections = simulation.connections[pysimgrid.simdag.TaskKind.TASK_KIND_COMM_E2E]

.. autoclass:: pysimgrid.simdag.TaskKind
  :members:
  :undoc-members:

Task states
===========

Task state is provided by the underlying library and can be used to conveniently filter tasks.

Let's explain by example::

  import pysimgrid.simdag

  with pysimgrid.simdag.Simulation(platform, "task_file.dot") as simulation:
    # select all schedulable tasks
    schedulable = simulation.tasks[pysimgrid.simdag.TaskState.TASK_STATE_SCHEDULABLE]
    # select all running OR ready to run tasks
    states = (pysimgrid.simdag.TaskState.TASK_STATE_RUNNING,
              pysimgrid.simdag.TaskState.TASK_STATE_RUNNABLE,
              pysimgrid.simdag.TaskState.TASK_STATE_SCHEDULED)
    ready_or_running = simulation.tasks[states]
    # select all completed tasks
    done = simulation.tasks[pysimgrid.simdag.TaskState.TASK_STATE_DONE]
    # select all fully transfered connections
    conn_done = simulation.connections[pysimgrid.simdag.TaskState.TASK_STATE_DONE]


.. autoclass:: pysimgrid.simdag.TaskState
   :members:
   :undoc-members:
