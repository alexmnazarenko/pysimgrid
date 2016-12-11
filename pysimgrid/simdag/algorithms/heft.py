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

from .. import scheduler
from ..taskflow import Taskflow
from ... import cscheduling


class HEFTScheduler(scheduler.StaticScheduler):
  """
  Implementation of a famous Heterogeneous Earliest Finish Time (HEFT) scheduling algorithm.

  Many advantages of this method include:
  * pretty high performance
  * low (N**2M) time complexity
  * quite simple implementation

  The general idea is very simple:
  1. Sort tasks according in decreasing ranku order

     ranku(task) = ECT(task) + max_among_children(ranku(child) + ECOMT(task, child))

     where ECT and ECOMT are evaluated using platform mean speed, bandwidt and latency values

     Important property of this ordering is that it is also an topological order (see below).

  2. Go over ordered task and schedule each one to the minimize task completion time.

     To estimate task completion time, ofc, you need to all task parents to be scheduled already.
     This is achieved automatically as HEFT order is also a topological sort for a tasks.

     HEFT scheduling allows insertions to happen, so if some host has an empty and wide enough time slot,
     next task may be added in between already scheduled tasks.

  For more details please refer to the original HEFT publication:
    H. Topcuoglu, S. Hariri and Min-You Wu, "Performance-effective and low-complexity task
    scheduling for heterogeneous computing", IEEE Transactions on Parallel and Distributed
    Systems, Vol 13, No 3, 2002, pp. 260-274
  """

  def get_schedule(self, simulation):
    """
    Overriden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    state = cscheduling.SchedulerState(simulation)

    ordered_tasks = self.heft_order(nxgraph, platform_model)

    self.heft_schedule(nxgraph, platform_model, state, ordered_tasks)
    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan

  @classmethod
  def heft_order(cls, nxgraph, platform_model):
    """
    Order task according to HEFT ranku.

    Params:
      nxgraph: full task graph as networkx.DiGraph
      platform_model: see HEFTScheduler.platform_model

    Returns:
      a list of tasks in a HEFT order
    """
    mean_speed, mean_bandwidth, mean_latency = platform_model.mean_speed, platform_model.mean_bandwidth, platform_model.mean_latency
    task_ranku = {}
    for task in networkx.topological_sort(nxgraph, reverse=True):
      ecomt_and_rank = [
        task_ranku[child] + (edge["weight"] / mean_bandwidth + mean_latency)
        for child, edge in nxgraph[task].items()
      ] or [0]
      task_ranku[task] = task.amount / mean_speed + max(ecomt_and_rank)
    # use node name as an additional sort condition to deal with zero-weight tasks (e.g. root)
    return sorted(nxgraph.nodes(), key=lambda node: (task_ranku[node], node.name), reverse=True)

  @classmethod
  def heft_schedule(cls, nxgraph, platform_model, state, ordered_tasks):
    """
    Build a HEFT schedule for a given state.
    Implemented as a separate function to be used in lookahead scheduler.

    Note: modifies a given state inplace

    Params:
      nxgraph: full task graph as networkx.DiGraph
      platform_model: cscheduling.PlatformModel object
      state: cscheduling.SchedulerState object
      ordered_tasks: tasks in a HEFT order
    """
    for task in ordered_tasks:
      current_min = cscheduling.MinSelector()
      for host, timesheet in state.timetable.items():
        est = platform_model.est(host, nxgraph.pred[task].items(), state)
        eet = platform_model.eet(task, host)
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
        # strange key order to ensure stable sorting:
        #  first sort by ECT (as HEFT requires)
        #  if equal - sort by host speed
        #  if equal - sort by host name (guaranteed to be unique)
        current_min.update((finish, host.speed, host.name), (host, pos, start, finish))
      host, pos, start, finish = current_min.value
      #print(task.name, host.name, pos, est, start, finish)
      state.update(task, host, pos, start, finish)
    return state
