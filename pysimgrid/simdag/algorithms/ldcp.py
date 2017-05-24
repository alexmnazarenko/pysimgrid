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
import operator
from ..scheduler import StaticScheduler
from queue import Queue


class LDCP(StaticScheduler):
  def __init__(self, simulation):
    super(self.__class__, self).__init__(simulation)
    self._platform_model = cscheduling.PlatformModel(self._simulation)
    self._state = cscheduling.SchedulerState(self._simulation)
    self._bandwidth = self.mean_bandwidth
    self._nxgraph = self._simulation.get_task_graph()
    networkx.set_node_attributes(
      self._nxgraph,
      'num_unscheduled_parents',
      {task: len(self._nxgraph.predecessors(task)) for task in self._nxgraph.nodes_iter()}
    )
    self._schedulable_tasks = set(
      task for task in self._nxgraph.nodes_iter()
      if self._nxgraph.node[task]['num_unscheduled_parents'] == 0
    )
    self._scheduled_tasks = set()
    self._unscheduled_tasks = set(self._nxgraph.nodes_iter())
    self._temporary_edges = {host: set() for host in self.hosts}
    self._dagp = {host: self._simulation.get_task_graph() for host in self.hosts}
    for host, graph in self._dagp.items():
      networkx.set_node_attributes(
        graph,
        'size',
        {task: task.amount / host.speed for task in graph}
      )
    self._urank = {host: {} for host in self.hosts}
    self.update_urank(initial=True)
    self._task_to_schedule = None
    self._host_to_schedule = None
    self._last_identified_task = None
    self._last_used_host_dagp = None

  @property
  def hosts(self):
    """
    :return: list of hosts in simulation system
    """
    container_type = type(self._simulation.hosts)
    return container_type(host for host in self._simulation.hosts if not cscheduling.is_master_host(host))

  @property
  def mean_bandwidth(self):
    """
    :return: mean bandwidth in simulation system
    """
    return self._platform_model.bandwidth.mean()

  @property
  def timetable(self):
    """
    :return: timetable for available hosts
    """
    return self._state.timetable

  @staticmethod
  def pair_to_key(pair):
    """
    :param pair: (key, value)
    :return: key
    """
    return operator.itemgetter(0)(pair)

  @staticmethod
  def pair_to_value(pair):
    """
    :param pair: (key, value)
    :return: value
    """
    return operator.itemgetter(1)(pair)

  @staticmethod
  def timesheet_to_tasks(timesheet):
    """
    :param timesheet: timesheet of scheduled tasks for some host: [(task, start, finish), ...]
    :return: generator of tasks obtained by removing start and finish times: [task1, task2, ...]
    """
    return map(operator.itemgetter(0), timesheet)

  def get_schedule(self, simulation):
    """
    Overridden method that computes timetable for scheduling and expected makespan.
    :param simulation: description of simulating system
    :return: tuple that contains scheduling plan and expected makespan (sec.)
    """
    for _ in self._nxgraph.nodes_iter():
      self.select_task_to_schedule()
      if self.try_schedule_boundary_task():
        continue
      self.select_host_to_schedule()
      self.update_size_wrt_selected_task()
      self.update_communications_costs()
      self.update_execution_constraints()
      self.update_zero_cost_edges_on_dagp_wrt_selected_host()
      self.update_urank()
    expected_makespan = max(state['ect'] for state in self._state.task_states.values())
    return self._state.schedule, expected_makespan

  def get_uras_with_term(self, task, host, use_only_unscheduled=False):
    """
    :param task: task in computation graph
    :param host: host, which DAGP is used to compute URAS of task
    :param use_only_unscheduled: boolean keyword param, that is True, when it is only unscheduled successors are
      considered, otherwise False. Defaults to False.
    :return: for input and internal nodes: pair (uras, urank), where uras is a node in DAGP[host]
      with maximal sum of URank and communication cost; (None, 0) for nodes without successors
    """
    graph = self._dagp[host]
    successors = set(graph.successors(task))
    if use_only_unscheduled:
      successors.intersection_update(self._unscheduled_tasks)
    successors_with_communication_cost_and_urank = (
      (child, graph.edge[task][child]['weight'] / self._bandwidth + self._urank[host][child])
      for child in successors
    )
    uras, uras_term = max(successors_with_communication_cost_and_urank, key=self.pair_to_value) if len(successors) > 0 \
      else (None, 0.)
    return uras, uras_term

  def update_urank(self, initial=False):
    """
    :param initial: boolean param, that is True, when it is needed to compute URank of all nodes,
      and False, when only scheduled tasks are considered for updating URank values. Defaults to False.
    :return: None
    """
    for host in self.hosts:
      for task in networkx.topological_sort(self._dagp[host], reverse=True):
        if not initial and task in self._unscheduled_tasks:
          continue
        _, uras_term = self.get_uras_with_term(task, host)
        self._urank[host][task] = self._dagp[host].node[task]['size'] + uras_term

  def get_node_predecessors(self, graph, node, condition=None):
    """
    Generator, that yields predecessors of node in directed acyclic graph.
    :param graph: graph in which successors are obtained
    :param node: node whose predecessors are needed
    :param condition: if not None, then only nodes satisfying specified condition are considered,
      else all nodes are active. Defaults to None.
    :return: yields node predecessors of node in graph
    """
    visited, queue = set(), Queue()
    queue.put_nowait(node)
    while not queue.empty():
      node = queue.get_nowait()
      for pred in graph.predecessors(node):
        if pred not in visited:
          if condition is None or condition(pred):
            yield pred
            queue.put_nowait(pred)
          visited.add(pred)

  def get_schedulable_predecessors(self, graph, node):
    """
    :param graph: DAGP in consideration
    :param node: node which predecessors are needed
    :return: generator of nodes in graph corresponding to unscheduled tasks,
      which are predecessors of specified node and has no unscheduled predecessors (i.e. ready for scheduling)
    """
    unscheduled_predecessors = self.get_node_predecessors(graph, node, condition=lambda t: t in self._unscheduled_tasks)
    schedulable_predecessors = filter(lambda t: t in self._schedulable_tasks, unscheduled_predecessors)
    return schedulable_predecessors

  def update_schedulable_status(self):
    """
    Procedure of updating status: task to schedule is removed from the set of nodes, and its successors may be added
      if all the parents became scheduled
    :return: None
    """
    self._schedulable_tasks.remove(self._task_to_schedule)
    for child in self._nxgraph.successors(self._task_to_schedule):
      self._nxgraph.node[child]['num_unscheduled_parents'] -= 1
      if self._nxgraph.node[child]['num_unscheduled_parents'] == 0:
        self._schedulable_tasks.add(child)

  def select_task_to_schedule(self):
    """
    Procedure of selecting next task to schedule according to the provided algorithm
    :return: None
    """
    if self._last_identified_task is None:
      top_node = networkx.topological_sort(self._nxgraph)[0]
      top_node_host_uranks = ((host, self._urank[host][top_node]) for host in self.hosts)
      self._task_to_schedule = top_node
      self._last_identified_task = top_node
      self._last_used_host_dagp = self.pair_to_key(max(top_node_host_uranks, key=self.pair_to_value))
    else:
      uras_with_term = self.get_uras_with_term(self._last_identified_task, self._last_used_host_dagp,
                                               use_only_unscheduled=True)
      key_node = self.pair_to_key(uras_with_term)
      key_node_host_uranks = ((host, self._urank[host][key_node]) for host in self.hosts)
      key_host = self.pair_to_key(max(key_node_host_uranks, key=self.pair_to_value))
      self._last_used_host_dagp = key_host
      if key_node in self._schedulable_tasks:
        self._task_to_schedule = key_node
        self._last_identified_task = key_node
      else:
        schedulable_predecessors = self.get_schedulable_predecessors(self._dagp[key_host], key_node)
        urank_predecessors_values = ((node, self._urank[key_host][node]) for node in schedulable_predecessors)
        parent_key_node = self.pair_to_key(max(urank_predecessors_values, key=self.pair_to_value))
        self._task_to_schedule = parent_key_node
    self.update_schedulable_status()
    self._scheduled_tasks.add(self._task_to_schedule)
    self._unscheduled_tasks.remove(self._task_to_schedule)

  def try_schedule_boundary_task(self):
    """
    :return: result of trying to schedule task as boundary: True, when task in input / output, False otherwise.
    """
    return cscheduling.try_schedule_boundary_task(self._task_to_schedule, self._platform_model, self._state)

  def select_host_to_schedule(self):
    """
    Procedure of selecting host to schedule the chosen task according to the provided algorithm
    :return: None
    """
    current_min = cscheduling.MinSelector()
    for host, timesheet in self._state.timetable.items():
      if cscheduling.is_master_host(host):
        continue
      est = self._platform_model.est(host, self._nxgraph.pred[self._task_to_schedule], self._state)
      eet = self._platform_model.eet(self._task_to_schedule, host)
      pos, start, finish = cscheduling.timesheet_insertion(timesheet, est, eet)
      current_min.update((finish, host.speed, host.name), (host, pos, start, finish))
    self._host_to_schedule, pos, start, finish = current_min.value
    self._state.update(self._task_to_schedule, self._host_to_schedule, pos, start, finish)

  def update_size_wrt_selected_task(self):
    """
    Set the size of nodes corresponding to selected task for the value of its size of the selected host.
      This must be done for each DAGP.
    :return: None
    """
    task_size_wrt_selected_task = self._dagp[self._host_to_schedule].node[self._task_to_schedule]['size']
    for graph in self._dagp.values():
      graph.node[self._task_to_schedule]['size'] = task_size_wrt_selected_task

  def update_communications_costs(self):
    """
    For each parent scheduled on the same host as of the selected task, weight of edge between them is set to zero
    :return: None
    """
    tasks_on_selected_host = set(self.timesheet_to_tasks(self.timetable[self._host_to_schedule]))
    predecessors_on_selected_host = tasks_on_selected_host.intersection(
      self._nxgraph.predecessors(self._task_to_schedule)
    )
    for graph in self._dagp.values():
      for pred in predecessors_on_selected_host:
        graph.edge[pred][self._task_to_schedule]['weight'] = 0.

  def update_execution_constraints(self):
    """
    Consider a task to schedule: if there is a task scheduled right before on the same processor, it should be connected
      to the current task to schedule by zero-cost edge, the same procedure should be done for the task right after
      the current task to schedule. if there are both task scheduled right before and right after it
      on the same processor, then, the previously added zero-cost edge between them should be removed.
    :return: None
    """
    tasks = list(self.timesheet_to_tasks(self.timetable[self._host_to_schedule]))
    task_to_schedule_index = tasks.index(self._task_to_schedule)
    timesheet_predecessor = tasks[task_to_schedule_index - 1] if task_to_schedule_index > 0 else None
    timesheet_successor = tasks[task_to_schedule_index + 1] if task_to_schedule_index < len(tasks) - 1 else None
    for graph in self._dagp.values():
      if timesheet_predecessor is not None and timesheet_successor is not None:
        graph.remove_edge(timesheet_predecessor, timesheet_successor)
      if timesheet_predecessor is not None:
        graph.add_edge(timesheet_predecessor, self._task_to_schedule, weight=0.)
      if timesheet_successor is not None:
        graph.add_edge(self._task_to_schedule, timesheet_successor, weight=0.)

  def update_zero_cost_edges_on_dagp_wrt_selected_host(self):
    """
    Temporary edges from the task last scheduled on the selected host to all the ready to schedule nodes that do not
      communicate with this task. This must be done after removing the previous temporary zero-cost edges in DAGP,
      corresponding to the selected host.
    :return: None
    """
    for from_task, to_task in self._temporary_edges[self._host_to_schedule]:
      self._dagp[self._host_to_schedule].remove_edge(from_task, to_task)
    self._temporary_edges[self._host_to_schedule].clear()
    last_scheduled_task = list(self.timesheet_to_tasks(self.timetable[self._host_to_schedule]))[-1]
    successors = self._dagp[self._host_to_schedule].successors(last_scheduled_task)
    for task in self._schedulable_tasks.difference(successors):
      self._dagp[self._host_to_schedule].add_edge(last_scheduled_task, task, weight=0.)
      self._temporary_edges[self._host_to_schedule].add((last_scheduled_task, task))
