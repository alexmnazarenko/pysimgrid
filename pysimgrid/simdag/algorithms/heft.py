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

import networkx
import numpy

from .. import scheduler
from ..taskflow import Taskflow
from ... import cplatform
from . import utils


class HEFTScheduler(scheduler.StaticScheduler):

  def get_schedule(self, simulation):
    """
    Overriden.
    """
    nxgraph = simulation.get_task_graph()
    platform_model = self.platform_model(simulation)

    task_states = {task: {"ect": numpy.nan, "host": None} for task in simulation.tasks}
    schedule = {host: [] for host in simulation.hosts}
    ordered_tasks = self.heft_order(nxgraph, platform_model)

    schedule = self.heft_schedule(nxgraph, platform_model, task_states, schedule, ordered_tasks)
    clean_schedule = {host: [task for (task, _, _) in timesheet] for (host, timesheet) in schedule.items()}
    return clean_schedule

  @classmethod
  def platform_model(cls, simulation):
    """
    Extract platform linear model. Required for ranku calculation and optimized scheduling.

    Params:
      simulation: pysimgrid.simdag.Simulation object
    """
    hosts = simulation.hosts
    bandwidth_matrix = numpy.zeros((len(hosts), len(hosts)))
    latency_matrix = numpy.zeros((len(hosts), len(hosts)))
    for i, src in enumerate(hosts):
      for j in range(i+1, len(hosts)):
        dst = simulation.hosts[j]
        bandwidth_matrix[i,j] = bandwidth_matrix[j,i] = cplatform.route_bandwidth(src, dst)
        latency_matrix[i,j] = latency_matrix[j,i] = cplatform.route_latency(src, dst)

    return {
      "bandwidth": bandwidth_matrix,
      "latency": latency_matrix,
      "hosts_map": {host: idx for (idx, host) in enumerate(hosts)},
      "mean_speed": numpy.mean([h.speed for h in simulation.hosts]),
      "mean_comm": bandwidth_matrix.sum() / (bandwidth_matrix.size - bandwidth_matrix.shape[0]),
      "mean_lat": latency_matrix.sum() / (latency_matrix.size - latency_matrix.shape[0])
    }

  @classmethod
  def heft_order(cls, nxgraph, platform_model):
    """
    Return tasks in a HEFT ranku order.

    Params:
      nxgraph: tasks as nxgraph.DiGraph
      platform_model: see HEFTScheduler.platform_model
    """
    mean_speed, mean_bandwidth, mean_latency = platform_model["mean_speed"], platform_model["mean_comm"], platform_model["mean_lat"]
    task_ranku = {}
    task_toporder = {}
    # use topological_order as an additional sort condition to deal with zero-weight tasks (e.g. root)
    #   bonus: add topsort_order to ensure stable topological_sort across python runs
    topsort_order = sorted(nxgraph.nodes(), key=lambda t: t.name)
    for idx, task in enumerate(networkx.topological_sort(nxgraph, topsort_order, reverse=True)):
      ecomt_and_rank = [
        task_ranku[child] + (edge["weight"] / mean_bandwidth + mean_latency)
        for child, edge in nxgraph[task].items()
      ] or [0]
      task_ranku[task] = task.amount / mean_speed + max(ecomt_and_rank)
      task_toporder[task] = idx
    return sorted(nxgraph.nodes(), key=lambda node: (task_ranku[node], task_toporder[node]), reverse=True)

  @classmethod
  def heft_schedule(cls, nxgraph, platform_model, task_states, schedule, ordered_tasks):
    for task in ordered_tasks:
      possible_schedules = []
      for host, timesheet in schedule.items():
        est_by_parent = [utils.parent_data_ready_time(task, parent, host, edge_dict, task_states, platform_model)
                         for parent, edge_dict in nxgraph.pred[task].items()] or [0]
        est = max(est_by_parent)
        eet = task.get_eet(host)
        pos, start, finish = utils.timesheet_insertion(timesheet, est, eet)
        # strange key order to ensure stable sorting:
        #  first sort by ECT (as HEFT requires)
        #  if equal - sort by host speed
        #  if equal - sort by host name (guaranteed to be unique)
        possible_schedules.append((finish, host.speed, host.name, host, pos, start, finish))
      host, pos, start, finish = min(possible_schedules)[3:]
      utils.update_schedule_state(task, host, pos, start, finish, task_states, schedule)
    return schedule
