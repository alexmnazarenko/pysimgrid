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

from ... import cscheduling
from ..scheduler import StaticScheduler


class HCPT(StaticScheduler):
  """
  Heterogeneous Critical Parent Trees (HCPT) scheduler.

  HCPT is based on heuristics to find a critical path in worflow and schedule it with
  the highest possible priority.

  The key part is 'guided' topological sort which finds a schedulable order selecting cricial
  nodes as early as possible.

  For more details and rationale please refer to the original publication:

    T. Hagras and J. Janecek, "A Simple Scheduling Heuristic for Heterogeneous Computing Environments",
    Proceedings of the Second International Symposium on Parallel and Distributed Computing (ISPDC’03),
    2003, pp. 104-110
  """
  def get_schedule(self, simulation):
    """
    Overridden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    state = cscheduling.SchedulerState(simulation)
    tasks_aest, tasks_alst = self.get_tasks_aest_alst(nxgraph, platform_model)

    # All nodes in the critical path must have AEST=ALST
    critical_path = sorted(
      [(t, tasks_aest[t]) for t in tasks_aest if numpy.isclose(tasks_aest[t], tasks_alst[t])],
      key=lambda x: x[1]
    )
    critical_path.reverse()

    stack = deque([elem[0] for elem in critical_path])
    queue = deque()
    while len(stack):
      task = stack.pop()
      parents = nxgraph.pred[task]
      untracked_parents = sorted(
        set(parents) - set(queue),
        key=lambda x: tasks_aest[x]
      )
      if untracked_parents:
        stack.append(task)
        for parent in untracked_parents:
          stack.append(parent)
      else:
        queue.append(task)

    scheduled = set()
    while len(queue):
      task = queue.popleft()
      if task in scheduled:
        continue
      else:
        scheduled.add(task)
      if cscheduling.try_schedule_boundary_task(task, platform_model, state):
        continue
      current_min = cscheduling.MinSelector()
      for host, timesheet in state.timetable.items():
        if cscheduling.is_master_host(host):
          continue
        est = platform_model.est(host, dict(nxgraph.pred[task]), state)
        eet = platform_model.eet(task, host)
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
        # strange key order to ensure stable sorting:
        #  first sort by ECT
        #  if equal - sort by host speed
        #  if equal - sort by host name (guaranteed to be unique)
        current_min.update((finish, host.speed, host.name), (host, pos, start, finish))
      host, pos, start, finish = current_min.value
      state.update(task, host, pos, start, finish)

    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan

  @classmethod
  def get_tasks_aest_alst(cls, nxgraph, platform_model):
    """
    Return AEST and ALST of tasks.

    Args:
      nxgraph: full task graph as networkx.DiGraph
      platform_model: cscheduling.PlatformModel object

    Returns:
      tuple containg 2 dictionaries
        aest: task->aest_value
        alst: task->alst_value
    """
    mean_speed = platform_model.mean_speed
    mean_bandwidth = platform_model.mean_bandwidth
    mean_latency = platform_model.mean_latency
    topological_order = list(networkx.topological_sort(nxgraph))

    # Average execution cost
    aec = {task: float(task.amount) / mean_speed for task in nxgraph}

    # Average earliest start time
    aest = {}
    # TODO: Check several roots and ends!
    root = topological_order[0]
    end = topological_order[-1]
    aest[root] = 0.
    for task in topological_order:
      parents = nxgraph.pred[task]
      if not parents:
        aest[task] = 0
        continue
      aest[task] = max([
        aest[parent] + aec[parent] + (nxgraph[parent][task]["weight"] / mean_bandwidth + mean_latency)
        for parent in parents
      ])

    topological_order.reverse()

    # Average latest start time
    alst = {}
    alst[end] = aest[end]
    for task in topological_order:
      if not nxgraph[task]:
        alst[task] = aest[task]
        continue
      alst[task] = min([
        alst[child] - (edge["weight"] / mean_bandwidth + mean_latency)
        for child, edge in nxgraph[task].items()
      ]) - aec[task]

    return aest, alst
