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


import copy
import itertools
import numpy

from .. import scheduler
from . import heft
from . import utils


class LookaheadScheduler(scheduler.StaticScheduler):

  def get_schedule(self, simulation):
    nxgraph = simulation.get_task_graph()
    platform_model = heft.HEFTScheduler.platform_model(simulation)

    task_states = {task: {"ect": numpy.nan, "host": None} for task in simulation.tasks}
    schedule = {host: [] for host in simulation.hosts}
    ordered_tasks = heft.HEFTScheduler.heft_order(nxgraph, platform_model)

    for idx, task in enumerate(ordered_tasks):
      possible_schedules = []
      for host, timesheet in schedule.items():
        # manual copy of initial state
        # copy.deepcopy is slow as hell
        temp_task_states = {task: dict(state) for (task, state) in task_states.items()}
        temp_schedule = {host: list(timesheet) for (host, timesheet) in schedule.items()}

        est_by_parent = [utils.parent_data_ready_time(task, parent, host, edge_dict, task_states, platform_model)
                         for parent, edge_dict in nxgraph.pred[task].items()] or [0]
        est = max(est_by_parent)
        eet = task.get_eet(host)
        pos, start, finish = utils.timesheet_insertion(timesheet, est, eet)
        utils.update_schedule_state(task, host, pos, start, finish, temp_task_states, temp_schedule)
        heft.HEFTScheduler.heft_schedule(nxgraph, platform_model, temp_task_states, temp_schedule, ordered_tasks[(idx + 1):])
        total_time = max([state["ect"] for state in temp_task_states.values()])
        possible_schedules.append((total_time, host.speed, host.name, host, pos, start, finish))
      host, pos, start, finish = min(possible_schedules)[3:]
      utils.update_schedule_state(task, host, pos, start, finish, task_states, schedule)
    clean_schedule = {host: [task for (task, _, _) in timesheet] for (host, timesheet) in schedule.items()}
    return clean_schedule
