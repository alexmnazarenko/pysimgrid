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

from ... import cscheduling
from .. import scheduler
from . import heft


class Lookahead(scheduler.StaticScheduler):
  """
  Lookahead scheduler.

  Lookahead aims to be an improved version of HEFT, avoiding some of its greedy scheduling decisions.

  Some quick experiments show that it can indeed improve on HEFT schedules, but it comes with a price:
  lookahead scheduler is much slower (as it runs HEFT to an end for each task/host pair).

  Main idea is simple:

    1. Consider tasks in a HEFT (ranku) order
    2. For each next task and for all hosts:

      * schedule task on a host
      * complete schedule using HEFT
      * evaluate makespan

    3. Select a host achieving a best makespan

  Note: authors actually propose 4 variants of the algorithm and as for now only the first one is implemented.

  For more details please refer to the original publication:

    L. F. Bittencourt, R. Sakellariou and E. R. M. Madeira, "DAG Scheduling Using a Lookahead
    Variant of the Heterogeneous Earliest Finish Time Algorithm", 18th Euromicro
    Conference on Parallel, Distributed and Network-based Processing, 2010, pp. 27-34
  """
  def get_schedule(self, simulation):
    """
    Overriden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    state = cscheduling.SchedulerState(simulation)

    ordered_tasks = cscheduling.heft_order(nxgraph, platform_model)
    for idx, task in enumerate(ordered_tasks):
      if cscheduling.try_schedule_boundary_task(task, platform_model, state):
        continue
      current_min = cscheduling.MinSelector()
      for host, timesheet in state.timetable.items():
        if cscheduling.is_master_host(host):
          continue
        temp_state = state.copy()
        est = platform_model.est(host, dict(nxgraph.pred[task]), state)
        eet = platform_model.eet(task, host)
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
        temp_state.update(task, host, pos, start, finish)
        cscheduling.heft_schedule(nxgraph, platform_model, temp_state, ordered_tasks[(idx + 1):])
        total_time = max([state["ect"] for state in temp_state.task_states.values()])
        # key order to ensure stable sorting:
        #  first sort by HEFT makespan (as Lookahead requires)
        #  if equal - sort by host speed
        #  if equal - sort by host name (guaranteed to be unique)
        current_min.update((total_time, host.speed, host.name), (host, pos, start, finish))
      host, pos, start, finish = current_min.value
      state.update(task, host, pos, start, finish)

    expected_makespan = max([state["ect"] for state in state.task_states.values()])
    return state.schedule, expected_makespan