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
from .. import six
from .. import csimdag
from .. import cplatform

class Scheduler(six.with_metaclass(abc.ABCMeta)):
  def __init__(self, simulation):
    self._simulation = simulation
    self._log = logging.getLogger(type(self).__name__)

  @abc.abstractmethod
  def run(self):
    raise NotImplementedError()

  def _check_done(self):
    unfinished = self._simulation.all_tasks.by_prop("state", csimdag.TASK_STATE_DONE, True)
    if any(unfinished):
      raise Exception("some tasks are not finished by the end of simulation: {}".format([t.name for t in unfinished]))


class StaticScheduler(Scheduler):
  def __init__(self, simulation):
    self._simulation = simulation

  def run(self):
    schedule = self.get_schedule(self._simulation)
    if not isinstance(schedule, dict):
      raise Exception("'get_schedule' must return a dictionary")
    for host, task_list in schedule.items():
      if not (isinstance(host, cplatform.Host) and isinstance(task_list, list)):
        raise Exception("'get_schedule' must return a dictionary Host:List_of_tasks")

    unscheduled = self._simulation.tasks[csimdag.TASK_STATE_NOT_SCHEDULED, csimdag.TASK_STATE_SCHEDULABLE]
    if set(itertools.chain.from_iterable(schedule.values())) != set(self._simulation.tasks):
      raise Exception("some tasks are left unscheduled by static algorithm: {}".format([t.name for t in unscheduled]))
    if len(unscheduled) != len(self._simulation.tasks):
      raise Exception("static scheduler should not directly schedule tasks")

    hosts_status = {h: True for h in self._simulation.hosts}

    for t in self._simulation.tasks:
      t.watch(csimdag.TASK_STATE_DONE)

    changed = self._simulation.tasks.by_func(lambda t: False)
    while True:
      self.__update_host_status(hosts_status, changed)
      self.__schedule_to_free_hosts(schedule, hosts_status)
      changed = self._simulation.simulate()
      if not changed:
        break

    self._check_done()

  @abc.abstractmethod
  def get_schedule(self, simulation):
    raise NotImplementedError()

  def __update_host_status(self, hosts_status, changed):
    for t in changed.by_prop("kind", csimdag.TASK_KIND_COMM_E2E, True)[csimdag.TASK_STATE_DONE]:
      for h in t.hosts:
        hosts_status[h] = True

  def __schedule_to_free_hosts(self, schedule, hosts_status):
    for host in schedule:
      if schedule[host] and hosts_status[host] == True:
        task = schedule[host].pop(0)
        task.schedule(host)
        hosts_status[host] = False


class DynamicScheduler(Scheduler):
  def run(self):
    self.prepare(self._simulation)
    for t in self._simulation.tasks:
      t.watch(csimdag.TASK_STATE_DONE)
    # a bit ugly kludge - cannot just pass an empty list there, needs to be a _TaskList
    self.schedule(self._simulation, self._simulation.all_tasks.by_func(lambda t: False))
    changed = self._simulation.simulate()
    while changed:
      self.schedule(self._simulation, changed)
      changed = self._simulation.simulate()
    self._check_done()

  @abc.abstractmethod
  def prepare(self, simulation):
    raise NotImplementedError()

  @abc.abstractmethod
  def schedule(self, simulation, changed):
    raise NotImplementedError()
