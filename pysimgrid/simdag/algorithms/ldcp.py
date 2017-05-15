# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it wil l be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# along with this library.  If not, see <http://www.gnu.org/licenses/>.
#

import networkx
from ... import cscheduling
from copy import deepcopy
import operator
from ..scheduler import StaticScheduler


class LDCP(StaticScheduler):
  def __init__(self):
    super().__init__(self)
    self.simulation = None
    self.nxgraph = None
    self.dagp = None
    self.platform_model = None
    self.state = None
    self.urank = None
    self.uras = None
    self.task_to_schedule = None
    self.host_to_schedule = None

  @property
  def hosts(self):
    return self.simulation.hosts

  @property
  def mean_bandwidth(self):
    return self.platform_model.bandwidth.mean()

  @property
  def timetable(self):
    return self.state.timetable

  def get_schedule(self, simulation):
    """
    Overridden.
    """
    self.simulation = simulation
    self.platform_model = cscheduling.PlatformModel(simulation)
    self.state = cscheduling.SchedulerState(simulation)
    self.nxgraph = self.simulation.get_task_graph()
    self.init_dagps()
    self.get_urank()
    self.uras = networkx.topological_sort(self.nxgraph, reverse=True)[0]
    for _ in range(len(self.nxgraph)):
      self.select_task_to_schedule()
      if cscheduling.try_schedule_boundary_task(self.task_to_schedule, self.platform_model, self.state):
        continue
      self.select_host_to_schedule()
      self.update_size_wrt_selected_task()
      self.update_communications_costs()
      self.update_execution_constraints()
      self.update_zero_cost_edges_on_dagp_wrt_selected_processor()
      self.update_urank()
    expected_makespan = max(state['ect'] for state in self.state.task_states.values())
    return self.state.schedule, expected_makespan

  def init_dagps(self):
    self.dagp = {host: deepcopy(self.nxgraph) for host in self.hosts}
    for host in self.dagp.keys():
      for task in self.dagp[host]:
        task['num_unscheduled_parents'] = len(self.nxgraph.pred[task])
        task.amout /= host.speed

  def get_urank(self):
    # mean_bandwidth = 1 for debugging due to the article
    self.urank = {}
    for host in self.simulation.hosts:
      self.urank[host] = {}
      nxgraph = self.dagp[host]
      for task in networkx.topological_sort(nxgraph, reverse=True):
        urank_succ_max = max(
          child['weight'] / self.mean_bandwidth + self.urank[host][child]
          for child in nxgraph.succ[task]
        )
        self.urank[host][task] = task.amout / host.speed + urank_succ_max

  def select_task_to_schedule(self):
    urank_values = {host: self.urank[host][self.uras] for host in self.hosts}
    key_host = max(urank_values.items(), key=operator.itemgetter(1))[0]
    key_node = self.dagp[key_host][self.uras]
    if self.num_unscheduled_parents(key_node) == 0:
      self.task_to_schedule = key_node
    else:
      pred = self.dagp[key_host].pred[key_node]
      urank_pred_values = {node: self.urank[key_host][node] for node in pred}
      parent_key_node = max(urank_pred_values, key=operator.itemgetter(1))[0]
      self.task_to_schedule = parent_key_node
      self.nxgraph[key_node]['num_unscheduled_parents'] -= 1

  def select_host_to_schedule(self):
    current_min = cscheduling.MinSelector()
    for host, timesheet in self.state.timetable.items():
      if cscheduling.is_master_host(host):
        continue
      est = self.platform_model.est(host, self.nxgraph.pred[self.task_to_schedule], self.state)
      eet = self.platform_model.eet(self.task_to_schedule, host)
      pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
      current_min.update((finish, host.speed, host.name), (host, pos, start, finish))
    self.host_to_schedule, pos, start, finish = current_min.value
    self.state.update(self.task_to_schedule, self.host_to_schedule, pos, start, finish)

  def update_size_wrt_selected_task(self):
    for graph in self.dagp.values():
      graph[self.task_to_schedule].amount = self.dagp[self.host_to_schedule][self.task_to_schedule].amount

  @staticmethod
  def timetable_to_tasks(timetable):
    return map(operator.itemgetter(0), timetable)

  def update_communications_costs(self):
    predecessors = set(self.nxgraph.pred[self.task_to_schedule])
    parent_to_zero_cost_update = set(self.timetable_to_tasks(self.timetable[self.host_to_schedule])) & predecessors
    for graph in self.dagp.values():
      for parent in parent_to_zero_cost_update:
        graph[parent][self.task_to_schedule]['weight'] = 0.

  def update_execution_constraints(self):
    tasks = list(self.timetable_to_tasks(self.timetable[self.host_to_schedule]))
    task_to_schedule_index = tasks.index(self.task_to_schedule)
    timetable_predecessor = tasks[task_to_schedule_index - 1] if task_to_schedule_index > 0 else None
    timetable_successor = tasks[task_to_schedule_index + 1] if task_to_schedule_index < len(tasks) - 1 else None
    for graph in self.dagp.values():
      if timetable_predecessor and timetable_successor:
        graph.remove_edge(timetable_predecessor, timetable_successor)
      if timetable_predecessor:
        graph.add_edge(timetable_predecessor, self.task_to_schedule)
      if timetable_successor:
        graph.add_edge(self.task_to_schedule, timetable_successor)

  def update_zero_cost_edges_on_dagp_wrt_selected_processor(self):
    pass

  def update_urank(self):
    pass
