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
import numpy as np

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

        self.host_tasks = {}
        self._exec_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME, True)
        for host in self._exec_hosts:
            self.host_tasks[host.name] = []

        self._target_hosts = {}
        self._is_free = {}
        if self._master_host:
            for task in simulation.tasks.by_func(lambda t: t.name in self.BOUNDARY_TASKS):
                task.schedule(self._master_host)

    def schedule(self, simulation, changed):
        clock = simulation.clock

        for h in simulation.hosts:
            self._is_free[h] = True
        for task in simulation.tasks[csimdag.TaskState.TASK_STATE_RUNNING, csimdag.TaskState.TASK_STATE_SCHEDULED]:
            self._is_free[task.hosts[0]] = False

        schedulable = simulation.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]
        unscheduled = schedulable.by_func(lambda task: self._target_hosts.get(task) is None)

        num_unscheduled = len(unscheduled)

        # build ECT matrix
        ECT = np.zeros((num_unscheduled, len(self._exec_hosts)))
        for t, task in enumerate(unscheduled):
            for h, host in enumerate(self._exec_hosts):
                ECT[t][h] = self.get_ect(clock, task, host)

        # build schedule
        task_idx = np.arange(num_unscheduled)
        for _ in range(0, num_unscheduled):
            min_hosts = np.argmin(ECT, axis=1)
            min_times = ECT[np.arange(ECT.shape[0]), min_hosts]

            if ECT.shape[1] > 1:
                min2_times = np.partition(ECT, 1)[:, 1]
                sufferages = min2_times - min_times
            else:
                sufferages = -min_times

            possible_schedules = []
            for i in range(0, len(task_idx)):
                best_host_idx = int(min_hosts[i])
                best_ect = min_times[i]
                sufferage = sufferages[i]
                possible_schedules.append((i, best_host_idx, best_ect, sufferage))

            t, h, ect = self.batch_heuristic(possible_schedules)
            task = unscheduled[int(task_idx[t])]
            host = self._exec_hosts[h]

            self._target_hosts[task] = host
            self.host_tasks[host.name].append(task)
            # logging.info("%s -> %s" % (task.name, host.name))
            task_time = ect - host.data["est"]
            host.data["est"] = ect

            task_idx = np.delete(task_idx, t)
            ECT = np.delete(ECT, t, 0)
            ECT[:,h] += task_time

        for host in self._exec_hosts:
            if self._is_free[host]:
                tasks = self.host_tasks[host.name]
                if len(tasks) > 0:
                    task = tasks.pop(0)
                    task.schedule(host)
                    # logging.info("%s -> %s" % (task.name, host.name))

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
        return min(possible_schedules, key=lambda s: (s[2], s[0]))[:-1]


class BatchMax(BatchScheduler):
    """
    Batch-mode MaxMin scheduler.

    Schedules all currently schedulable tasks to a best host by ECT in a single batch.

    The order in a batch is determined by a heuristic:
    MaxMin prioritizes the tasks with maximum ECT on a best host
    """

    def batch_heuristic(self, possible_schedules):
        return max(possible_schedules, key=lambda s: (s[2], s[0]))[:-1]


class BatchSufferage(BatchScheduler):
    """
    Batch-mode Sufferage scheduler.

    Schedules all currently schedulable tasks to a best host by ECT in a single batch.

    The order in a batch is determined by a heuristic:
    Sufferage prioritizes tasks with maximum difference between ECT on 2 best hosts
    """

    def batch_heuristic(self, possible_schedules):
        return max(possible_schedules, key=lambda s: (s[3], s[0]))[:-1]
