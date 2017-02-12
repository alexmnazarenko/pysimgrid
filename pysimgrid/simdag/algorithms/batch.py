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


from .. import scheduler
from ... import csimdag


class BatchScheduler(scheduler.DynamicScheduler):
  """
  Batch-mode heuristic base implementation.

  Schedules all currently schedulable tasks to a best host by ECT in a single batch.

  The order in a batch is determined by a heuristic:

  * MinMin prioritizes the tasks with minimum ECT on a best host

  * MaxMin prioritizes the tasks with maximum ECT on a best host

  * Sufferage prioritizes tasks with maximum difference between ECT on 2 best hosts
  """
  def prepare(self, simulation):
    for h in simulation.hosts:
      h.data = {
        "est": 0.
      }
    master_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME)
    self._master_host = master_hosts[0] if master_hosts else None
    self._exec_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME, True)
    self._target_hosts = {}
    self._is_free = {}
    if self._master_host:
      for task in simulation.tasks.by_func(lambda t: t.name in self.BOUNDARY_TASKS):
        task.schedule(self._master_host)

  def schedule(self, simulation, changed):
    for h in simulation.hosts:
      self._is_free[h] = True
    for task in simulation.tasks[csimdag.TaskState.TASK_STATE_RUNNING, csimdag.TaskState.TASK_STATE_SCHEDULED]:
      self._is_free[task.hosts[0]] = False
    clock = simulation.clock
    schedulable = simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]
    unscheduled = schedulable.by_func(lambda task: self._target_hosts.get(task) is None)
    for _ in range(len(unscheduled)):
      possible_schedules = []
      for task in unscheduled:
        ects = {h: self.get_ect(clock, task, h) for h in self._exec_hosts}
        sorted_hosts = self._exec_hosts.sorted(lambda h: ects[h])
        best_host = sorted_hosts[0]
        best_ect = sufferage = ects[best_host]
        if len(sorted_hosts) > 1:
          sufferage -= ects[sorted_hosts[1]]
        possible_schedules.append((task, best_host, best_ect, sufferage))
      task, target_host, ect = self.batch_heuristic(possible_schedules)
      self._target_hosts[task] = target_host
      target_host.data["est"] = ect
      # filter out scheduled task
      unscheduled = unscheduled.by_prop("native", task.native, True)
    for task in schedulable:
      target_host = self._target_hosts[task]
      if self._is_free[target_host]:
        task.schedule(target_host)
        self._is_free[target_host] = False

  @staticmethod
  def get_ect(clock, task, host):
    parent_connections = [p for p in task.parents if p.kind == csimdag.TaskKind.TASK_KIND_COMM_E2E]
    comm_times = [conn.get_ecomt(conn.parents[0].hosts[0], host) for conn in parent_connections]
    return max(host.data["est"], clock) + task.get_eet(host) + (max(comm_times) if comm_times else 0.)


class BatchMin(BatchScheduler):
  """
  Batch-mode MinMin scheduler.

  Schedules all currently schedulable tasks to a best host by ECT in a single batch.

  The order in a batch is determined by a heuristic:
  MinMin prioritizes the tasks with minimum ECT on a best host.
  """
  def batch_heuristic(self, possible_schedules):
    return min(possible_schedules, key=lambda s: (s[2], s[0].name))[:-1]


class BatchMax(BatchScheduler):
  """
  Batch-mode MaxMin scheduler.

  Schedules all currently schedulable tasks to a best host by ECT in a single batch.

  The order in a batch is determined by a heuristic:
  MaxMin prioritizes the tasks with maximum ECT on a best host
  """
  def batch_heuristic(self, possible_schedules):
    return max(possible_schedules, key=lambda s: (s[2], s[0].name))[:-1]


class BatchSufferage(BatchScheduler):
  """
  Batch-mode Sufferage scheduler.

  Schedules all currently schedulable tasks to a best host by ECT in a single batch.

  The order in a batch is determined by a heuristic:
  Sufferage prioritizes tasks with maximum difference between ECT on 2 best hosts
  """
  def batch_heuristic(self, possible_schedules):
    return max(possible_schedules, key=lambda s: (s[3], s[0].name))[:-1]
