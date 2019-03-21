# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# along with this library.  If not, see <http://www.gnu.org/licenses/>.
#

import abc
import itertools
import logging
import os
import time

from enum import Enum

from .. import six
from .. import csimdag
from .. import cplatform


class TaskExecutionMode(Enum):
  """
  Execution mode defines how tasks are executed on a host.

  - SEQUENTIAL (default):
    task are executed strictly one by one, in order specified by the scheduler.

  - PARALLEL:
    tasks can execute in parallel, host speed is fairly shared between concurrent tasks.
  """
  SEQUENTIAL = 1
  PARALLEL = 2


class DataTransferMode(Enum):
  """
  Data transfer strategy defines when and in what order data transfers, corresponding to edges in a workflow DAG,
  are scheduled during the workflow execution. For each data transfer, the source task is called producer and
  the destination task is called consumer. Applicable for SEQUENTIAL execution mode only.

  - EAGER (default):
    Data transfer is scheduled when the data is ready, i.e. the producer is completed,
    and the destination node is known, i.e. the consumer is scheduled.

  - LAZY:
    Data transfer is scheduled when the destination node is ready to execute the consumer task.

  - PREFETCH:
    Data transfer is scheduled when the destination node is ready to execute a task
    immediately preceding the consumer task.

  - QUEUE:
    Data transfers on each destination node are scheduled sequentially in the order of planned execution
    of consumer tasks on this node.

  - QUEUE_ECT:
    Data transfers on each destination node are scheduled sequentially in the order of expected completion time
    of producer tasks, breaking the ties with the order of planned execution of consumer tasks.

  - PARENTS:
    Data transfer is scheduled when all parents of the consumer task are completed.

  - LAZY_PARENTS:
    Combination of LAZY and PARENTS strategies.
  """
  EAGER = 1
  LAZY = 2
  PREFETCH = 3
  QUEUE = 4
  QUEUE_ECT = 5
  PARENTS = 6
  LAZY_PARENTS = 7


class Scheduler(six.with_metaclass(abc.ABCMeta)):
  """
  Base class for all scheduling algorithms.

  Defines scheduler public interface and provides (very few) useful methods for
  actual schedulers:

    *self._log* - Logger object (see logging module documentation)

    *self._check_done()* - raise an exception if there are any unfinished tasks
  """
  BOUNDARY_TASKS = ["root", "end"]
  MASTER_HOST_NAME = "master"

  def __init__(self, simulation):
    """
    Initialize scheduler instance.

    Args:
      simulation: a :class:`pysimgrid.simdag.Simulation` object
    """
    self._simulation = simulation
    self._log = logging.getLogger(type(self).__name__)

    # Task execution and data transfer modes are configured via environment variables.
    if "PYSIMGRID_TASK_EXECUTION" in os.environ:
      self._task_exec_mode = TaskExecutionMode[os.environ["PYSIMGRID_TASK_EXECUTION"]]
    else:
      self._task_exec_mode = TaskExecutionMode.SEQUENTIAL
    if "PYSIMGRID_DATA_TRANSFER" in os.environ:
      self._data_transfer_mode = DataTransferMode[os.environ["PYSIMGRID_DATA_TRANSFER"]]
    else:
      self._data_transfer_mode = DataTransferMode.EAGER

    algo = type(self).__name__
    if self._data_transfer_mode == DataTransferMode.QUEUE_ECT:
      if algo not in ['HEFT', 'Lookahead']:
        raise Exception('%s does not support %s mode' % (algo, self._data_transfer_mode))

  @abc.abstractmethod
  def run(self):
    """
    Descibes the simulation process.
    Single call to this method should perform the full
    simulation, scheduling all the tasks and calling the
    simulation API as necessary.

    Note:
      Not intended to be overriden in the concrete algorithms.
    """
    raise NotImplementedError()

  @abc.abstractproperty
  def scheduler_time(self):
    """
    Wall clock time spent scheduling.
    """
    raise NotImplementedError()

  @abc.abstractproperty
  def total_time(self):
    """
    Wall clock time spent scheduling and simulating.
    """
    raise NotImplementedError()

  @property
  def expected_makespan(self):
    """
    Algorithm's makespan prediction. Can return None if algorithms didn't/cannot provide it.
    """
    return None

  def _check_done(self):
    """
    Check that all tasks are completed after the simulation.

    Useful to transform SimGrid's unhappy logs into actual detectable error.
    """
    unfinished = self._simulation.all_tasks.by_prop("state", csimdag.TASK_STATE_DONE, True)
    if any(unfinished):
      raise Exception("some tasks are not finished by the end of simulation: {}".format([t.name for t in unfinished]))

  @classmethod
  def is_boundary_task(cls, task):
    return (task.name in cls.BOUNDARY_TASKS) and task.amount == 0


class StaticScheduler(Scheduler):
  """
  Base class for static scheduling algorithms.

  Provides some non-trivial functionality, ensuring that tasks scheduled on the same host
  do not execute concurrently.
  """
  def __init__(self, simulation):
    super(StaticScheduler, self).__init__(simulation)
    self.__scheduler_time = -1.
    self.__total_time = -1.
    self.__expected_makespan = None

  def run(self):
    """
    Execute a static schedule produced by algorithm implementation.
    """
    start_time = time.time()
    schedule = self.get_schedule(self._simulation)
    self.__scheduler_time = time.time() - start_time
    self._log.debug("Scheduling time: %f", self.__scheduler_time)
    if not isinstance(schedule, (dict, tuple)):
      raise Exception("'get_schedule' must return a dictionary or a tuple")
    if isinstance(schedule, tuple):
      if len(schedule) != 2 or not isinstance(schedule[0], dict) or not isinstance(schedule[1], float):
        raise Exception("'get_schedule' returned tuple should have format (<schedule>, <expected_makespan>)")
      schedule, self.__expected_makespan = schedule
      self._log.debug("Expected makespan: %f", self.__expected_makespan)
    for host, task_list in schedule.items():
      if not (isinstance(host, cplatform.Host) and isinstance(task_list, list)):
        raise Exception("'get_schedule' must return a dictionary Host:List_of_tasks")

    unscheduled = self._simulation.tasks[csimdag.TASK_STATE_NOT_SCHEDULED, csimdag.TASK_STATE_SCHEDULABLE]
    if set(itertools.chain.from_iterable(schedule.values())) != set(self._simulation.tasks):
      raise Exception("some tasks are left unscheduled by static algorithm: {}".format([t.name for t in unscheduled]))
    if len(unscheduled) != len(self._simulation.tasks):
      raise Exception("static scheduler should not directly schedule tasks")

    # schedule tasks according to task execution and data transfer modes
    for host, tasks in schedule.items():
      if self._data_transfer_mode in [DataTransferMode.QUEUE, DataTransferMode.QUEUE_ECT]:
        data_transfers = []

      for pos, task in enumerate(tasks):
        task.schedule(host)

        # do not add any constraints for boundary tasks
        if task.name in self.BOUNDARY_TASKS:
          continue

        # do not add any constraints for PARALLEL mode
        if self._task_exec_mode == TaskExecutionMode.PARALLEL:
          continue

        prev_task = schedule[host][pos - 1] if pos > 0 else None
        prev2_task = schedule[host][pos - 2] if pos > 1 else None

        # SEQUENTIAL task execution mode:
        # forbid task overlapping by adding dependency on previous task
        if prev_task is not None:
          parent_tasks = set()
          for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
            parent_tasks.add(comm.parents[0])
          if prev_task not in parent_tasks:
            self._simulation.add_dependency(prev_task, task)

        # data transfer modes
        if self._data_transfer_mode == DataTransferMode.EAGER:
          # no additional dependencies are needed
          pass
        elif self._data_transfer_mode in [DataTransferMode.LAZY, DataTransferMode.LAZY_PARENTS]:
          # add dependency from previous task to input data transfer
          if prev_task is not None:
            for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
              if comm.parents[0] != prev_task:
                self._simulation.add_dependency(prev_task, comm)
        elif self._data_transfer_mode == DataTransferMode.PREFETCH:
          # add dependency from previous task data transfer to input data transfer
          if prev_task is not None:
            for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
              for prev_comm in prev_task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
                self._simulation.add_dependency(prev_comm, comm)
          # add dependency from pre-previous task to input data transfer
          # (this ensures that data transfer starts only when previous task is ready to run!)
          if prev2_task is not None:
            for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
              if comm.parents[0] != prev2_task:
                self._simulation.add_dependency(prev2_task, comm)
        elif self._data_transfer_mode == DataTransferMode.QUEUE:
          # build a list of host inbound data transfers
          for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
            data_transfers.append((comm, pos))
        elif self._data_transfer_mode == DataTransferMode.QUEUE_ECT:
          # build a list of host inbound data transfers
          for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
            data_transfers.append((comm, (comm.parents[0].data["ect"], pos)))

      if self._data_transfer_mode in [DataTransferMode.QUEUE, DataTransferMode.QUEUE_ECT]:
        # form a queue from host inbound data transfers
        data_transfers.sort(key=lambda t: t[1])
        prev_comm = None
        for comm, _ in data_transfers:
          if prev_comm is not None:
            self._simulation.add_dependency(prev_comm, comm)
          prev_comm = comm

    # PARENTS / LAZY_PARENTS modes
    # (separate loop to avoid breaking the data transfers)
    if self._data_transfer_mode in [DataTransferMode.PARENTS, DataTransferMode.LAZY_PARENTS]:
      for host, tasks in schedule.items():
        for pos, task in enumerate(tasks):
          if task.name in self.BOUNDARY_TASKS:
            continue
          if self._task_exec_mode == TaskExecutionMode.PARALLEL:
            continue
          # add dependency from parents to input data transfers
          for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
            for comm2 in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
              if comm2 != comm:
                parent = comm2.parents[0]
                if comm.parents[0] != parent:
                  self._simulation.add_dependency(parent, comm)

    # make sure that our manipulations with dependencies do not break the data transfers
    for task in self._simulation.tasks:
      task_host = task.hosts[0]
      for comm in task.parents[csimdag.TaskKind.TASK_KIND_COMM_E2E]:
        parent_task = comm.parents[0]
        parent_task_host = parent_task.hosts[0]
        src = comm.hosts[0]
        dst = comm.hosts[1] if len(comm.hosts) == 2 else src
        if src != parent_task_host or dst != task_host:
          raise Exception("Sanity check FAILED! Data transfer: %s [%s] -> %s [%s] has wrong hosts: %s -> %s"
                          % (parent_task.name, parent_task_host.name, task.name, task_host.name, src.name, dst.name))

    self._simulation.simulate()
    self._check_done()
    self.__total_time = time.time() - start_time

  @abc.abstractmethod
  def get_schedule(self, simulation):
    """
    Abstract method that need to be overriden in scheduler implementation.

    Args:
      simulation: a :class:`pysimgrid.simdag.Simulation` object

    Returns:

      Expected to return a schedule as dict {host -> [list_of_tasks...]}.
      Optionally, can also return a predicted makespan. Then return type is a tuple (schedule, predicted_makespan_in_seconds).
    """
    raise NotImplementedError()

  @property
  def scheduler_time(self):
    return self.__scheduler_time

  @property
  def total_time(self):
    return self.__total_time

  @property
  def expected_makespan(self):
    return self.__expected_makespan


class DynamicScheduler(Scheduler):
  """
  Base class for dynamic scheduling algorithms.
  """
  def __init__(self, simulation):
    super(DynamicScheduler, self).__init__(simulation)
    self.__scheduler_time = -1.
    self.__total_time = -1.

  def run(self):
    start_time = time.time()
    self.prepare(self._simulation)
    for t in self._simulation.tasks:
      t.watch(csimdag.TASK_STATE_DONE)
    scheduler_time = time.time()
    # a bit ugly kludge - cannot just pass an empty list there, needs to be a _TaskList
    self.schedule(self._simulation, self._simulation.all_tasks.by_func(lambda t: False))
    self.__scheduler_time = time.time() - scheduler_time
    changed = self._simulation.simulate()
    while changed:
      scheduler_time = time.time()
      self.schedule(self._simulation, changed)
      self.__scheduler_time += time.time() - scheduler_time
      changed = self._simulation.simulate()
    self._check_done()
    self.__total_time = time.time() - start_time

  @abc.abstractmethod
  def prepare(self, simulation):
    """
    Abstract method that need to be overriden in scheduler implementation.

    Executed once before the simulation. Can be used to setup initial state for tasks and hosts.

    Args:
      simulation: a :class:`pysimgrid.simdag.Simulation` object
    """
    raise NotImplementedError()

  @abc.abstractmethod
  def schedule(self, simulation, changed):
    """
    Abstract method that need to be overriden in scheduler implementation.

    Args:
      simulation: a :class:`pysimgrid.simdag.Simulation` object
      changed: a list of changed tasks
    """
    raise NotImplementedError()

  @property
  def scheduler_time(self):
    return self.__scheduler_time

  @property
  def total_time(self):
    return self.__total_time
