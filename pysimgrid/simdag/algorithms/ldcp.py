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
  """
  Longest Dynamic Critical Path scheduler.
  
  Provided algorithms based on idea of critical paths (CP), that recomputes on every scheduling step: Dynamic CP.
  On every step the longest path is exploited: this must be done for prioritizing more resource demanding tasks.
  More strict definition for this kind of CP is provided below:
  Given a DAG with n tasks and e edges, and a HeDCS (Heterogeneous Distributed Computation System) with m heterogeneous
  processors, the LDCP during a particular scheduling step is a path of tasks and edges from an entry tasks to an exit
  task that has the largest sum of communication costs of edges and computation costs of tasks over all processors.
  Communication costs between tasks scheduled on the same processor are assumed zero, and the execution constraints are
  preserved.
  
  DAGP_j is a graph related to the host_j
  
  Upward rank of node n_i in a task graph DAGP_j, denoted as URank_j(n_i), is recursively defined as
    URank_j(n_i) = w_j(n_i) + max_{n_k: immediate successors of n_i} (c_j(n_i, n_k) + URank_j(n_k)),
    where w_j is a size function of nodes for host_j, and c_j is a communication cost function for host_j.
    
  Upward rank associated successor (URAS) for node n_i in graph DAGP_j is a node, that maximizes second summand in
    URank definition; the rank of the exit node is equal to its size.
    
  During scheduling process, last identified task could be recursively defined as a task on LDCP that is
    URAS of last identified task on previous step: on the first step last identified task is absent due to starting
    the process of scheduling, on the second step last scheduled task is an input task with the maximal URank,
    on the next step last identified task is URAS of the input node with maximal URank. Also, if another step leads to
    scheduling of the node, that is not on the LDCP (i.e. its predecessor), then last identified task on the next step
    is the same on the current step.
    
  During scheduling process, last used host is a host that maximizes URank of last identified task.
    
  During scheduling process, key node is defined as URAS of the node associated with the last identified task in
    DAGP of the last used host. Host, that maximizes URank of key node indicates key DAGP, that is bound to this host.
    If the key node has unscheduled parents, then parent key node is a predecessors of this node that has
    the highest URank in key DAGP.
    
  Algorithm:
  
  while there are no unscheduled parents:
    find the key DAGP
    find the key node in the key DAGP
    if the key node has no unscheduled parents then
      identify the selected task using the key node
    else
      find the parent key node
      identify the selected task using the parent key node
    compute the finish time of the selected task on every processor in the system
    find the selected processor that minimizes the finish time of the selected task
    assign the selected task to the selected processor
    update the size of the nodes hat identify the selected task on all DAGPs
    update the communication costs on all DAGPs
    update the execution constraints on all DAGPs
    update the temporary ero-cost edges on the DAGP associated with the selected processor
    update the URank values of the nodes that identify the scheduled tasks on all DAGPs
  """
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
      pos, start, finish = self.select_host_to_schedule()
      self.assign_task_to_host(pos, start, finish)
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
      child_node = self._nxgraph.node[child]
      if child_node['num_unscheduled_parents'] > 0:
        child_node['num_unscheduled_parents'] -= 1
        if child_node['num_unscheduled_parents'] == 0:
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
      if key_node in self._schedulable_tasks:
        self._task_to_schedule = key_node
        self._last_identified_task = key_node
        self._last_used_host_dagp = key_host
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
    :return: tuple (pos, start, finish)
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
    return pos, start, finish

  def assign_task_to_host(self, pos, start, finish):
    """
    :param pos: position in timesheet for selected host to insert the task
    :param start: start time of the task
    :param finish: finish time of the task
    :return: None
    """
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

  def add_edge_safe(self, host, from_task, to_task, **edge_attributes):
    graph = self._dagp[host]
    edge_tuple = (from_task, to_task)
    if edge_tuple in self._temporary_edges[host]:
      self._temporary_edges[host].remove(edge_tuple)
    graph.add_edge(*edge_tuple, **edge_attributes)

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
    for host, graph in self._dagp.items():
      if timesheet_predecessor:
        self.add_edge_safe(host, timesheet_predecessor, self._task_to_schedule, weight=0.)
      if timesheet_successor:
        self.add_edge_safe(host, self._task_to_schedule, timesheet_successor, weight=0.)
      if timesheet_predecessor and timesheet_successor:
        graph.remove_edge(timesheet_predecessor, timesheet_successor)

  def update_zero_cost_edges_on_dagp_wrt_selected_host(self):
    """
    Creates temporary edges from the task last scheduled on the selected host to all the ready to schedule nodes that
      do not communicate with this task. This must be done after removing the previous temporary zero-cost edges
      in DAGP, corresponding to the selected host. Also, all temporary nodes to the currently scheduled tasks
      are removed from all DAGPs to prevent appearance of cycles.
    :return: None
    """
    for host, graph in self._dagp.items():
      edges_to_remove = set(
        edge_pair for edge_pair in self._temporary_edges[host]
        if self.pair_to_value(edge_pair) == self._task_to_schedule
      )
      self._temporary_edges[host].difference_update(edges_to_remove)
      for from_task, to_task in edges_to_remove:
        graph.remove_edge(from_task, to_task)
    graph = self._dagp[self._host_to_schedule]
    for from_task, to_task in self._temporary_edges[self._host_to_schedule]:
        graph.remove_edge(from_task, to_task)
    self._temporary_edges[self._host_to_schedule].clear()
    last_scheduled_task = list(self.timesheet_to_tasks(self.timetable[self._host_to_schedule]))[-1]
    successors = self._dagp[self._host_to_schedule].successors(last_scheduled_task)
    for task in self._schedulable_tasks.difference(successors):
      graph.add_edge(last_scheduled_task, task, weight=0.)
      self._temporary_edges[self._host_to_schedule].add((last_scheduled_task, task))
