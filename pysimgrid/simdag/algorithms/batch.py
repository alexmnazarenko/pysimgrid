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
        master_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME)
        self.master_host = master_hosts[0] if master_hosts else None
        self.exec_hosts = simulation.hosts.by_prop("name", self.MASTER_HOST_NAME, True)
        for h in self.exec_hosts:
            h.data = {
                "est": 0.,
                "is_free": True,
                "tasks": []
            }

        self.tasks = simulation.tasks
        for t in self.tasks:
            t.data = {
                "target_host": None
            }

        # schedule root/end tasks to master
        if self.master_host:
            for task in self.tasks.by_func(lambda t: t.name in self.BOUNDARY_TASKS):
                # logging.info("%s -> %s" % (task.name, self.master_host.name))
                task.schedule(self.master_host)

    def schedule(self, simulation, changed):
        clock = simulation.clock

        # mark freed hosts
        for task in changed:
            if task.kind == csimdag.TaskKind.TASK_KIND_COMP_SEQ and task.state == csimdag.TaskState.TASK_STATE_DONE:
                host = task.hosts[0]
                if host != self.master_host:
                    task.hosts[0].data["is_free"] = True

        # schedule unscheduled ready tasks
        unscheduled = self.tasks[csimdag.TaskState.TASK_STATE_SCHEDULABLE]\
                          .by_func(lambda task: task.data["target_host"] is None)
        num_unscheduled = len(unscheduled)
        if num_unscheduled > 0:

            # build ECT matrix
            ECT = np.zeros((num_unscheduled, len(self.exec_hosts)))
            for t, task in enumerate(unscheduled):
                parents = [(p, p.parents[0].hosts[0]) for p in task.parents if p.kind == csimdag.TaskKind.TASK_KIND_COMM_E2E]
                for h, host in enumerate(self.exec_hosts):
                    comm_times = [p_comm.get_ecomt(p_host, host) for (p_comm, p_host) in parents]
                    ECT[t][h] = max(host.data["est"], clock) + task.get_eet(host) + (max(comm_times) if comm_times else 0.)

            # build schedule
            task_idx = np.arange(num_unscheduled)
            for _ in range(0, num_unscheduled):
                min_hosts = np.argmin(ECT, axis=1)
                min_times = ECT[np.arange(ECT.shape[0]), min_hosts]

                if ECT.shape[1] > 1:
                    min2_times = np.partition(ECT, 1)[:, 1]
                    # round sufferage values to eliminate the influence of floating point errors
                    sufferages = np.round(min2_times - min_times, decimals=2)
                else:
                    # use min time for a single host case
                    sufferages = min_times

                possible_schedules = []
                for i in range(0, len(task_idx)):
                    best_host_idx = int(min_hosts[i])
                    best_ect = min_times[i]
                    sufferage = sufferages[i]
                    possible_schedules.append((i, best_host_idx, best_ect, sufferage))

                t, h, ect = self.batch_heuristic(possible_schedules)
                task = unscheduled[int(task_idx[t])]
                host = self.exec_hosts[h]
                # logging.info("%s -> %s" % (task.name, host.name))

                task.data["target_host"] = host.name
                host.data["tasks"].append(task)
                task_time = ect - max(host.data["est"], clock)
                host.data["est"] = ect

                task_idx = np.delete(task_idx, t)
                ECT = np.delete(ECT, t, 0)
                ECT[:,h] += task_time

        for host in self.exec_hosts:
            if host.data["is_free"]:
                tasks = host.data["tasks"]
                if len(tasks) > 0:
                    task = tasks.pop(0)
                    task.schedule(host)
                    host.data["is_free"] = False
                    # logging.info("%s -> %s" % (task.name, host.name))


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
        return max(possible_schedules, key=lambda s: (s[2], -s[0]))[:-1]


class BatchSufferage(BatchScheduler):
    """
    Batch-mode Sufferage scheduler.

    Schedules all currently schedulable tasks to a best host by ECT in a single batch.

    The order in a batch is determined by a heuristic:
    Sufferage prioritizes tasks with maximum difference between ECT on 2 best hosts
    """

    def batch_heuristic(self, possible_schedules):
        return max(possible_schedules, key=lambda s: (s[3], -s[0]))[:-1]
        # TODO: use ECT on a best host for breaking ties?
        # return max(possible_schedules, key=lambda s: (s[3], s[2], -s[0]))[:-1]
