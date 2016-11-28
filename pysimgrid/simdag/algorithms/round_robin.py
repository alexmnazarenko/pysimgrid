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

import random
from .. import scheduler
from .. import taskflow

class RoundRobinScheduler(scheduler.StaticScheduler):
  def get_schedule(self, simulation):
    schedule = {host: [] for host in simulation.hosts}
    ids_tasks = {task.name: task for task in simulation.tasks}
    taskflow = taskflow.Taskflow()
    taskflow.from_simulation(simulation)
    hosts_count = simulation.hosts
    for idx, task in enumerate(taskflow.topological_order()):
      schedule[simulation.hosts[idx % hosts_count]].append(ids_tasks[task])
    return schedule
