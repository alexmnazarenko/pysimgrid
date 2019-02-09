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


class DispatchMode(Enum):
  """
  Defines the strategy used for dispatching scheduled tasks to hosts (actually passing task assignments to SimDAG).

  FREE_HOST:
  - the task is dispatched to host only after all previously scheduled host tasks are completed

  IMMEDIATE:
  - the task is dispatched immediately but will execute only after all previously scheduled host tasks are completed
  - the dependency on the previous host task is added to DAG to ensure that task executions are not overlapping
  - this mode allows to start downloading input data as soon as possible

  PARENTS_DONE:
  - the task is dispatched only after all its parents are completed
  - the task will execute only after all previously scheduled host tasks are completed, similarly to IMMEDIATE mode

  IMMEDIATE_OVERLAP:
  - same as IMMEDIATE, but the tasks dispatched to the same host are allowed to execute concurrently
  """
  FREE_HOST = 1
  IMMEDIATE = 2
  PARENTS_DONE = 3
  IMMEDIATE_OVERLAP = 4


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

    # Dispatch mode is configured via environment variable.
    # Default mode is FREE_HOST.
    if "PYSIMGRID_DISPATCH_MODE" in os.environ:
      self.__dispatch_mode = DispatchMode[os.environ["PYSIMGRID_DISPATCH_MODE"]]
    else:
      self.__dispatch_mode = DispatchMode.FREE_HOST

  def run(self):
    """
    Execute a static schedule produced by algorithm implementation.
    """
    start_time = time.time()
    schedule = self.get_schedule(self._simulation)
    self.__scheduler_time = time.time() - start_time
    self._log.info("Scheduling time: %f", self.__scheduler_time)
    if not isinstance(schedule, (dict, tuple)):
      raise Exception("'get_schedule' must return a dictionary or a tuple")
    if isinstance(schedule, tuple):
      if len(schedule) != 2 or not isinstance(schedule[0], dict) or not isinstance(schedule[1], float):
        raise Exception("'get_schedule' returned tuple should have format (<schedule>, <expected_makespan>)")
      schedule, self.__expected_makespan = schedule
      self._log.info("Expected makespan: %f", self.__expected_makespan)
    for host, task_list in schedule.items():
      if not (isinstance(host, cplatform.Host) and isinstance(task_list, list)):
        raise Exception("'get_schedule' must return a dictionary Host:List_of_tasks")

    unscheduled = self._simulation.tasks[csimdag.TASK_STATE_NOT_SCHEDULED, csimdag.TASK_STATE_SCHEDULABLE]
    if set(itertools.chain.from_iterable(schedule.values())) != set(self._simulation.tasks):
      raise Exception("some tasks are left unscheduled by static algorithm: {}".format([t.name for t in unscheduled]))
    if len(unscheduled) != len(self._simulation.tasks):
      raise Exception("static scheduler should not directly schedule tasks")

    if self.__dispatch_mode == DispatchMode.FREE_HOST:
      hosts_status = {h: True for h in self._simulation.hosts}
    elif self.__dispatch_mode in [DispatchMode.IMMEDIATE, DispatchMode.IMMEDIATE_OVERLAP]:
      for host, tasks in schedule.items():
        prev = None
        for task in tasks:
          if prev is None or self.__dispatch_mode == DispatchMode.IMMEDIATE_OVERLAP:
            task.schedule(host)
          else:
            task.schedule_after(host, prev)
          prev = task
    elif self.__dispatch_mode == DispatchMode.PARENTS_DONE:
      task_assignments = {}
      for host, tasks in schedule.items():
        prev = None
        for task in tasks:
          task_assignments[task] = (host, prev)
          prev = task

    for t in self._simulation.tasks:
      t.watch(csimdag.TASK_STATE_DONE)

    changed = self._simulation.tasks.by_func(lambda t: False)
    while True:
      if self.__dispatch_mode == DispatchMode.FREE_HOST:
        self.__update_host_status(hosts_status, changed)
        self.__schedule_to_free_hosts(schedule, hosts_status)
      elif self.__dispatch_mode == DispatchMode.PARENTS_DONE:
        for task in self._simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]:
          host, prev = task_assignments[task]
          if prev is not None and prev.state < csimdag.TaskState.TASK_STATE_DONE:
            task.schedule_after(host, prev)
          else:
            task.schedule(host)

      changed = self._simulation.simulate()
      if not changed:
        break

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

  def __update_host_status(self, hosts_status, changed):
    for t in changed.by_prop("kind", csimdag.TASK_KIND_COMM_E2E, True)[csimdag.TASK_STATE_DONE]:
      for h in t.hosts:
        hosts_status[h] = True

  def __schedule_to_free_hosts(self, schedule, hosts_status):
    for host, tasks in schedule.items():
      if tasks and hosts_status[host] == True:
        task = tasks.pop(0)
        task.schedule(host)
        hosts_status[host] = False


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
