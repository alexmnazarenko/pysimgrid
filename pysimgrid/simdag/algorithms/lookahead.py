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


class LookaheadScheduler(scheduler.StaticScheduler):

  def get_schedule(self, simulation):
    """
    Overriden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = cscheduling.PlatformModel(simulation)
    state = cscheduling.SchedulerState(simulation)

    ordered_tasks = heft.HEFTScheduler.heft_order(nxgraph, platform_model)
    for idx, task in enumerate(ordered_tasks):
      current_min = cscheduling.MinSelector()
      for host, timesheet in state.timetable.items():
        temp_state = state.copy()
        est = platform_model.est(host, nxgraph.pred[task].items(), state)
        eet = platform_model.eet(task, host)
        pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
        temp_state.update(task, host, pos, start, finish)
        heft.HEFTScheduler.heft_schedule(nxgraph, platform_model, temp_state, ordered_tasks[(idx + 1):])
        total_time = max([state["ect"] for state in temp_state.task_states.values()])
        current_min.update((total_time, host.speed, host.name), (host, pos, start, finish))
      host, pos, start, finish = current_min.value
      state.update(task, host, pos, start, finish)
      
    return state.schedule
