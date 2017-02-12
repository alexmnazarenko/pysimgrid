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


class MCT(scheduler.DynamicScheduler):
  """
  Minimum completion time scheduler.

  Ready tasks are scheduled on host (probably busy one) that promises the earliest task completion.

  Task completion time is estimated as:

  ECT(task, host) = max(current_clock, estimated_host_ready_time) + max(comm_time_from_parents) + EET(task, host)
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

  def schedule(self, simulation, changed):
    for h in simulation.hosts:
      self._is_free[h] = True
    for task in simulation.tasks[csimdag.TaskState.TASK_STATE_RUNNING, csimdag.TaskState.TASK_STATE_SCHEDULED]:
      self._is_free[task.hosts[0]] = False
    clock = simulation.clock
    for task in simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]:
      target_host = self._target_hosts.get(task)
      if not target_host:
        if self.is_boundary_task(task) and self._master_host:
          target_host = self._master_host
        else:
          sorted_hosts = self._exec_hosts.sorted(lambda h: self.get_ect(clock, task, h))
          target_host = sorted_hosts[0]
        self._target_hosts[task] = target_host
        target_host.data["est"] = self.get_ect(clock, task, target_host)
      if self._is_free[target_host]:
        task.schedule(target_host)
        self._is_free[target_host] = False

  @staticmethod
  def get_ect(clock, task, host):
    parent_connections = [p for p in task.parents if p.kind == csimdag.TaskKind.TASK_KIND_COMM_E2E]
    comm_times = [conn.get_ecomt(conn.parents[0].hosts[0], host) for conn in parent_connections]
    return max(host.data["est"], clock) + task.get_eet(host) + (max(comm_times) if comm_times else 0.)
