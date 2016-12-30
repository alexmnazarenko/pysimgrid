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
from ... import cscheduling


class HEFT(scheduler.StaticScheduler):
  """
  Implementation of a famous Heterogeneous Earliest Finish Time (HEFT) scheduling algorithm.

  Many advantages of this method include:
  * pretty high performance
  * low time complexity (N**2M)
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

    ordered_tasks = cscheduling.heft_order(nxgraph, platform_model)

    cscheduling.heft_schedule(nxgraph, platform_model, state, ordered_tasks)
    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan
