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

  Algorithm is based on heuristics which use Dynamic Level to chose pair - host and task to assign task to this host.
  DL(Task_X, Host_Y, Timetable) = SL (Task_X) + D (Task_X, Host_Y) - max( DA(Task_X, Host_Y, Timetable), TF(Host_Y, Timetable) )

  SL(Task_X) (Task Static Level) is the largest sum of executions times (calculated using mean speed among all hosts)
  along any directed path from Task_X to final node (including Task_X execution time)

  TF(Host_Y, Timetable) - time when the last assigned finish execution on Host_Y.

  DA(Task_X, Host_Y, Timetable) - time when all parent tasks of Task_X will be finished and transferred to the Host_Y

  It is more easier to understand  "max( DA(Task_X, Host_Y, Timetable), TF(Host_Y, Timetable) )" as minimal time
  when we could start Task_X on Host_Y.

  D (Task_X, Host_Y) - difference between time execution of Task_X using mean speed and using Host_Y speed

  Desc:
  At the beginning of the algorithm find all of tasks which don't have undone parents so they are ready to execute.

  Lets define this as set of tasks as RT and WT as a ser of all other tasks.

  First calculate DL for every pair (Task, Host) where Task from RT and Host from all Hosts.

  Then on every step:

   1. Choose Task and Host with maximal DL and assign this Task to Host (choosing minimal available time to start execution)

   2. Delete assigned Task from RT.

   3. Recalculate DL for every pait (Task, Host) where Task from RT and Host is Host which we choose on the step one

   4. Find new Tasks from WT which don't have undone parents and delete them from WT and add them to RT.

   5. Calculated DL for all pairs : (new Task, Host) new Task - from Tasks added on prev. step, Host from all Hosts.



  For more details and rationale please refer to the original publication:

    Gilbert C. Sih, Edward A. Lee "A Compile-Time Scheduling Heuristic for Interconnection-Constrained Heterogeneous Processor Architectures",
    Transactions on Parallel and Distributed Systems (IEEE) 1993

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
    # unreal dynamic level - used to mark deleted on not set values in a queue
    unreal_dl = 1 + max(sl.items(), key=operator.itemgetter(1))[1] + max(aec.items(), key=operator.itemgetter(1))[1]
    dl = {host: {task: unreal_dl for task in nxgraph} for host in simulation.hosts}
    undone_parents = {task: len(nxgraph.pred[task]) for task in nxgraph}
    waiting_tasks = set(nxgraph)
    queue_tasks = set()
    for task in nxgraph:
      if undone_parents[task] == 0:
        for host in simulation.hosts:
          dl[host][task] = sl[task] + (task.amount / mean_speed - task.amount / host.speed)
        waiting_tasks.remove(task)
        queue_tasks.add(task)

    for iterations in range(len(nxgraph)):
      cur_max = unreal_dl
      task_to_schedule = -1
      host_to_schedule = -1
      for host in simulation.hosts:
        if cscheduling.is_master_host(host):
          continue
        for task in queue_tasks:
          if dl[host][task] == unreal_dl:
            continue
          if cur_max == unreal_dl or dl[host][task] > cur_max:
            cur_max = dl[host][task]
            host_to_schedule = host
            task_to_schedule = task

      assert (cur_max != unreal_dl)

      if cscheduling.try_schedule_boundary_task(task_to_schedule, platform_model, state) == False:
          est = platform_model.est(host_to_schedule, nxgraph.pred[task_to_schedule], state)
          eet = platform_model.eet(task_to_schedule, host_to_schedule)
          timesheet = state.timetable[host_to_schedule]
          pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
          state.update(task_to_schedule, host_to_schedule, pos, start, finish)

      new_tasks = set()
      for child, edge in nxgraph[task_to_schedule].items():
        undone_parents[child] -= 1
        if undone_parents[child] == 0:
          new_tasks.add(child)
          for host in simulation.hosts:
            dl[host][child] = self.calculate_dl(nxgraph, platform_model, state, sl, aec, child, host)
      
      for host in simulation.hosts:
          dl[host][task_to_schedule] = unreal_dl

      queue_tasks.remove(task_to_schedule)

      for task in queue_tasks:
        if undone_parents[task] == 0:
          dl[host_to_schedule][task] = self.calculate_dl(nxgraph, platform_model, state, sl, aec, task, host_to_schedule)

      for task in new_tasks:
        waiting_tasks.remove(task)
        queue_tasks.add(task)

    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan

  @classmethod
  def calculate_dl(cls, nxgraph, platform_model, state, sl, aec, task, host):
    est = platform_model.est(host, nxgraph.pred[task], state)
    eet = platform_model.eet(task, host)
    timesheet = state.timetable[host]
    pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
    return sl[task] + (aec[task] - task.amount / host.speed) - start

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

