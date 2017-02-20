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

from collections import deque

import networkx
import numpy
import operator

from ... import cscheduling
from ..scheduler import StaticScheduler


class DLS(StaticScheduler):
    """
    Heterogeneous Dynamic Level Scheduler.


    For more details and rationale please refer to the original publication:

      Author "A Compile-Time Scheduling Heuristic for Interconnection-Constrained Heterogeneous Processor Architectures",
      ??? (IEEE) 1993
    """

    def get_schedule(self, simulation):
        """
        Overridden.
        """
        nxgraph = simulation.get_task_graph()
        platform_model = cscheduling.PlatformModel(simulation)
        state = cscheduling.SchedulerState(simulation)

        mean_speed = platform_model.mean_speed
        aec, sl = self.get_tasks_sl_aec(nxgraph, platform_model)
        # Maximal Static Level which may occurs, using to  deleted or not available)
        UNREAL_DL = max(sl.items(), key=operator.itemgetter(1))[1] + max(aec.items(), key=operator.itemgetter(1))[1]
        # print(UNREAL_DL)
        dl = {host: {task: UNREAL_DL for task in nxgraph} for host in simulation.hosts}
        undone_parents = {task: len(nxgraph.pred[task]) for task in nxgraph}
        # print (simulation.hosts)
        for task in nxgraph:
            if undone_parents[task] == 0:
                for host in simulation.hosts:
                    dl[host][task] = sl[task] + (task.amount / mean_speed - task.amount / host.speed)
                    # print (host, task, dl[host][task], task.amount, platform_model.est(host, nxgraph.pred[task], state))

        not_scheduled_tasks = set(nxgraph)
        for iterations in range(len(nxgraph)):
            cur_max = UNREAL_DL
            task_to_schedule = -1
            host_to_schedule = -1
            for host in simulation.hosts:
                for task in nxgraph:
                    if dl[host][task] == UNREAL_DL:
                        continue
                    if cur_max == UNREAL_DL or dl[host][task] > cur_max:
                        cur_max = dl[host][task]
                        host_to_schedule = host
                        task_to_schedule = task

            assert (cur_max != UNREAL_DL)
            # print(cur_max, task_to_schedule, host_to_schedule)
            est = platform_model.est(host_to_schedule, nxgraph.pred[task_to_schedule], state)
            eet = platform_model.eet(task_to_schedule, host_to_schedule)
            timesheet = state.timetable[host_to_schedule]
            pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
            # print(est, eet, start)
            state.update(task_to_schedule, host_to_schedule, pos, start, finish)
            for child, edge in nxgraph[task_to_schedule].items():
                undone_parents[child] -= 1
                if undone_parents[child] == 0:
                    for host in simulation.hosts:
                        est = platform_model.est(host, nxgraph.pred[child], state)
                        eet = platform_model.eet(child, host)
                        timesheet = state.timetable[host]
                        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
                        dl[host][child] = sl[child] + (child.amount / mean_speed - child.amount / host.speed) - start

            for host in simulation.hosts:
                dl[host][task_to_schedule] = UNREAL_DL

            not_scheduled_tasks.remove(task_to_schedule)

            for task in not_scheduled_tasks:
                if undone_parents[task] == 0:
                    est = platform_model.est(host_to_schedule, nxgraph.pred[task], state)
                    eet = platform_model.eet(task, host_to_schedule)
                    timesheet = state.timetable[host_to_schedule]
                    pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
                    dl[host_to_schedule][task] = sl[task] + (
                    task.amount / mean_speed - task.amount / host_to_schedule.speed) - start

        return state.schedule

    @classmethod
    def get_tasks_sl_aec(cls, nxgraph, platform_model):
        """
        Return Average Execution Cost and Static Level for every task.

        Args:
          nxgraph: full task graph as networkx.DiGraph
          platform_model: cscheduling.PlatformModel object

        Returns:
            aec: task->aec_value
            sl: task->static_level_value
        """
        mean_speed = platform_model.mean_speed
        topological_order = networkx.topological_sort(nxgraph, reverse=True)

        # Average execution cost
        aec = {task: float(task.amount) / mean_speed for task in nxgraph}

        sl = {task: aec[task] for task in nxgraph}

        # Static Level
        for task in topological_order:
            for parent in nxgraph.pred[task]:
                sl[parent] = max(sl[parent], sl[task] + aec[parent])

        return aec, sl

