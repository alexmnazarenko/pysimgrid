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

import logging

from .. import scheduler
from ... import csimdag


class DynamicMCT(scheduler.DynamicScheduler):
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
        if self._master_host:
            for task in simulation.tasks.by_func(lambda t: t.name in self.BOUNDARY_TASKS):
                task.schedule(self._master_host)
        self._exec_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME, True)
        self._started_tasks = set()
        self._estimate_cache = {}

    def schedule(self, simulation, changed):
        clock = simulation.clock
        free_hosts = set(self._exec_hosts)
        for task in simulation.tasks[csimdag.TaskState.TASK_STATE_RUNNING, csimdag.TaskState.TASK_STATE_SCHEDULED]:
            host = task.hosts[0]
            free_hosts.discard(host)
            if task.start_time > 0 and task not in self._started_tasks:
                self._started_tasks.add(task)
                host.data["est"] = task.start_time + task.get_eet(host)
        host_est = {}
        for h in self._exec_hosts:
            host_est[h] = h.data["est"]
        for task in simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]:
            sorted_hosts = self._exec_hosts.sorted(lambda h: self.get_ect(host_est[h], clock, task, h))
            target_host = sorted_hosts[0]
            host_est[target_host] = self.get_ect(host_est[target_host], clock, task, target_host)
            if target_host in free_hosts:
                task.schedule(target_host)
                # logging.info("%s -> %s" % (task.name, target_host.name))
                target_host.data["est"] = self.get_ect(target_host.data["est"], clock, task, target_host)
                free_hosts.remove(target_host)
                if len(free_hosts) == 0:
                    break

    def get_ect(self, est, clock, task, host):
        if (task, host) in self._estimate_cache:
            task_time = self._estimate_cache[(task, host)]
        else:
            parent_connections = [p for p in task.parents if p.kind == csimdag.TaskKind.TASK_KIND_COMM_E2E]
            comm_times = [conn.get_ecomt(conn.parents[0].hosts[0], host) for conn in parent_connections]
            task_time = (max(comm_times) if comm_times else 0.) + task.get_eet(host)
            self._estimate_cache[(task, host)] = task_time
        return max(est, clock) + task_time
