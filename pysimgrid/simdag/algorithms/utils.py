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

import itertools

def timesheet_insertion(timesheet, est, eet):
  insert_index = len(timesheet)
  start_time = timesheet[-1][2] if timesheet else 0

  if timesheet:
    insertion = [(None, 0, 0)]
    extended = itertools.chain(insertion, timesheet)
    for idx, (t1, t2) in enumerate(zip(extended, timesheet)):
      slot = t2[1] - max(t1[2], est)
      if slot > eet:
        insert_index = idx
        start_time = t1[2]
        break

  start_time = max(start_time, est)
  return (insert_index, start_time, (start_time + eet))


def parent_data_ready_time(task, parent, target_host, edge_dict, task_states, platform_model):
  dst_idx = platform_model["hosts_map"][target_host]
  src_idx = platform_model["hosts_map"][task_states[parent]["host"]]
  if src_idx == dst_idx:
    return task_states[parent]["ect"]
  return task_states[parent]["ect"] + edge_dict["weight"] / platform_model["bandwidth"][src_idx][dst_idx] + platform_model["latency"][src_idx][dst_idx]


def update_schedule_state(task, host, pos, start, finish, task_states, schedule):
  # update task state
  task_state = task_states[task]
  task_state["ect"] = finish
  task_state["host"] = host
  # update timesheet
  schedule[host].insert(pos, (task, start, finish))
