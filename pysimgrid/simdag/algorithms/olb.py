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


class OLB(scheduler.DynamicScheduler):
    """
    Opportunistic load balancing scheduler.

    Dynamically schedule a free task on a fastest host, disregarding any communication costs.
    Main practical advantage is avoiding any attempts to estimate task performance on given machine.

    Can be considered baseline algorithm for benchmarking purposes as one of the simplest approaches
    that actually make sense for DAG scheduling.
    """

    def prepare(self, simulation):
        for h in simulation.hosts:
            h.data = {}
        master_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME)
        self._master_host = master_hosts[0] if master_hosts else None
        if self._master_host:
            for task in simulation.tasks.by_func(lambda t: t.name in self.BOUNDARY_TASKS):
                task.schedule(self._master_host)
        self._exec_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME, True)
        self._queue = []

    def schedule(self, simulation, changed):
        for h in simulation.hosts:
            h.data["free"] = True
        for task in simulation.tasks[csimdag.TaskState.TASK_STATE_RUNNING, csimdag.TaskState.TASK_STATE_SCHEDULED]:
            task.hosts[0].data["free"] = False
        queue_set = set(self._queue)
        for t in simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]:
            if t not in queue_set:
                self._queue.append(t)
        while self._queue:
            task = self._queue[0]
            free_hosts = self._exec_hosts.by_data("free", True)
            if free_hosts:
                self._queue.pop(0)
                free_hosts = free_hosts.sorted(lambda h: h.speed, reverse=True)
                target_host = free_hosts[0]
                task.schedule(target_host)
                # logging.info("%s -> %s" % (task.name, target_host.name))
                target_host.data["free"] = False
            else:
                break
