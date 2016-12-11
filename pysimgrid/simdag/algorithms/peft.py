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

import networkx
import numpy

from ... import cscheduling
from ..scheduler import StaticScheduler
from ..taskflow import Taskflow


class PEFTScheduler(StaticScheduler):
  """
  Implementation of a Predicted Earliest Finish Time (PEFT) algorithm.

  PEFT tries to combine benefits of HEFT and lookahead in a single algorithm.

  Instead of actual lookahead scheduling, PEFT pre-computes an Optimistic Cost Table (OCT),
  each element of which contains time to finish all the task descendants on their best hosts,
  disregarding the host availability.

  For more details and rationale please refer to the original publication:
    H. Arabnejad and J. G. Barbosa, "List Scheduling Algorithm for Heterogeneous Systems by
    an Optimistic Cost Table", IEEE Transactions on Parallel and Distributed Systems,
    Vol 25, No 3, 2014, pp. 682-694
  """
  def get_schedule(self, simulation):
    """
    Overriden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    state = cscheduling.SchedulerState(simulation)

    oct_dict = self.oct_dict(nxgraph, platform_model)
    oct_rank = {task: by_host.mean() for (task, by_host) in oct_dict.items()}

    ordered_tasks = cscheduling.schedulable_order(nxgraph, oct_rank)

    for task in ordered_tasks:
      current_min = cscheduling.MinSelector()
      for host, timesheet in state.timetable.items():
        est = platform_model.est(host, nxgraph.pred[task].items(), state)
        eet = platform_model.eet(task, host)
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
        # key order to ensure stable sorting:
        #  first sort by ECT + OCT (as PEFT requires)
        #  if equal - sort by host speed
        #  if equal - sort by host name (guaranteed to be unique)
        current_min.update((finish + oct_dict[task][platform_model.host_idx(host)], host.speed, host.name), (host, pos, start, finish))
      host, pos, start, finish = current_min.value
      state.update(task, host, pos, start, finish)
    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan

  @classmethod
  def oct_dict(cls, nxgraph, platform_model):
    """
    Build optimistic cost table as an dict task->array.

    Params:
      nxgraph: networkx representation of task graph
      platform_model: platform linear model
    """
    result = dict()
    for task in networkx.topological_sort(nxgraph, reverse=True):
      oct_row = numpy.zeros(platform_model.host_count)
      if not nxgraph[task]:
        result[task] = oct_row
        continue
      for host, idx in platform_model.host_map.items():
        child_results = []
        for child, edge_dict in nxgraph[task].items():
          row = result[child].copy()
          row += child.amount / platform_model.speed
          comm_cost = numpy.ones(platform_model.host_count) * edge_dict["weight"] / platform_model.mean_bandwidth
          comm_cost += platform_model.mean_latency
          comm_cost[idx] = 0
          row += comm_cost
          child_results.append(row.min())
        oct_row[idx] = max(child_results)
      result[task] = oct_row
    return result
