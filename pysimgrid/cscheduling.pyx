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

"""
Moreorless generic scheduling utils.

Some of those are called a lot so they benefit from a Cython usage.

However, there is still A LOT to optimize. Help is very welcome.
Cubic complexity scheduling algorithms like Lookahead are still painfully slow.

General optimization directions:
  * more type annotations
  * proper c-level numpy usage
  * less dict searches (may require schedulers update, which is undesirable)

General philosophy is simple - technical optimizations is OK, even if ugly. As long
as scheduler implementation doesn't suffer, this utilities may be as messed up as required.
For example, A TON of dict searches can be avoided if all task/host access will be performed by index.
However, it leads to ugly 'client' code (schedulers) and thus unacceptable.
"""


import networkx
import numpy

import cplatform

cimport numpy as cnumpy
cimport common
cimport cplatform
cimport csimdag


cdef class PlatformModel(object):
  """
  Platform linear model used for most static scheduling approaches.

  Disregards network topology.
  """
  cdef cnumpy.ndarray _speed
  cdef cnumpy.ndarray _bandwidth
  cdef cnumpy.ndarray _latency
  cdef double _mean_speed
  cdef double _mean_latency
  cdef double _mean_bandwidth
  cdef dict _host_map

  def __init__(self, simulation):
    hosts = simulation.hosts
    speed = numpy.zeros(len(hosts))
    bandwidth = numpy.zeros((len(hosts), len(hosts)))
    latency = numpy.zeros((len(hosts), len(hosts)))
    for i, src in enumerate(hosts):
      speed[i] = src.speed
      for j in range(i+1, len(hosts)):
        dst = simulation.hosts[j]
        bandwidth[i,j] = bandwidth[j,i] = cplatform.route_bandwidth(src, dst)
        latency[i,j] = latency[j,i] = cplatform.route_latency(src, dst)

    self._speed = speed
    self._bandwidth = bandwidth
    self._latency = latency
    self._mean_speed = speed.mean()
    self._mean_bandwidth = bandwidth.mean() * (bandwidth.size / (bandwidth.size - len(hosts)))
    self._mean_latency = latency.mean() * (latency.size / (bandwidth.size - len(hosts)))
    self._host_map = {host: idx for (idx, host) in enumerate(hosts)}

  @property
  def host_count(self):
    """
    Get platform host count.
    """
    return len(self._speed)

  @property
  def speed(self):
    """
    Get hosts speed as a vector.

    Refer to to host_map property or host_idx function to convert host instances to indices.
    """
    return self._speed

  @property
  def bandwidth(self):
    """
    Get platform connection bandwidths as a matrix.

    Note:
      For i==j bandwidth is 0
    """
    return self._bandwidth

  @property
  def latency(self):
    """
    Get platform connection latencies as a matrix.

    Note:
      For i==j latency is 0
    """
    return self._latency

  @property
  def mean_speed(self):
    """
    Get mean host speed in a platform.
    """
    return self._mean_speed

  @property
  def mean_bandwidth(self):
    """
    Get mean connection bandwidth in a platform.
    """
    return self._mean_bandwidth

  @property
  def mean_latency(self):
    """
    Get mean connection latency in a platform.
    """
    return self._mean_latency

  @property
  def host_map(self):
    """
    Get {Host: idx} mapping.
    """
    return self._host_map

  cpdef eet(self, csimdag.Task task, cplatform.Host host):
    """
    Calculate task eet on a given host.
    """
    return task.amount / host.speed

  cpdef parent_data_ready_time(self, cplatform.Host host, csimdag.Task parent, dict edge_dict, SchedulerState state):
    """
    Calculate data ready time for a single parent.

    Args:
      host: host on which a new (current) task will be executed
      parent: parent task
      edge_dict: edge properties dict (for now the only important property is "weight")
      state: current schedule state

    Return:
      earliest start time considering only a given parent
    """
    cdef dict task_states = state.task_states
    cdef int dst_idx = self._host_map[host]
    cdef int src_idx = self._host_map[task_states[parent]["host"]]
    if src_idx == dst_idx:
      return state.task_states[parent]["ect"]
    return task_states[parent]["ect"] + edge_dict["weight"] / self._bandwidth[src_idx, dst_idx] + self._latency[src_idx, dst_idx]

  cpdef est(self, cplatform.Host host, dict parents, SchedulerState state):
    """
    Calculate an earliest start time for a given task.

    Implementation is kind of dense, as it is the most critical function for
    HEFT/Lookahead algorithms execution time.

    Key points:

    * use numpy buffer types to speedup indexing
    * manually inline parent_data_ready_time function (synergistic with numpy usage. passing buffer types is costly for some reason)
    * annotate ALL types

    Args:
      host: host on which a new (current) task will be executed
      parents: iterable of parent tasks and egdes in a form [(parent, edge)...]
      state: current schedule state

    Returns:
      earliest start time as a float
    """
    cdef double result = 0.
    cdef double parent_time

    cdef dict task_states = state._task_states
    cdef dict task_state
    cdef int dst_idx = self._host_map[host]
    cdef int src_idx
    cdef csimdag.Task parent
    cdef dict edge_dict
    cdef double comm_amount

    # force ndarray types to ensure fast indexing
    cdef cnumpy.ndarray[double, ndim=2] bw = self._bandwidth
    cdef cnumpy.ndarray[double, ndim=2] lat = self._latency

    for parent, edge_dict in parents.items():
      task_state = task_states[parent]
      src_idx = self._host_map[task_state["host"]]
      if src_idx == dst_idx:
        parent_time =  task_state["ect"]
      else:
        comm_amount = edge_dict["weight"]
        # extract ect first to ensure it has fixed type
        # otherwise + operator will trigger nasty python lookup
        parent_time = task_state["ect"]
        parent_time += comm_amount / bw[src_idx, dst_idx] + lat[src_idx, dst_idx]
      if parent_time > result:
        result = parent_time
    return result

  def host_idx(self, cplatform.Host host):
    return self._host_map[host]


cdef class SchedulerState(object):
  """
  Stores the current scheduler state.

  See properties description for details.
  """
  cdef dict _task_states
  cdef dict _timetable

  def __init__(self, simulation=None, task_states=None, timetable=None):
    if simulation:
      if task_states or timetable:
        raise Exception("simulation is provided, initial state is not expected")
      self._task_states = {task: {"ect": numpy.nan, "host": None} for task in simulation.tasks}
      self._timetable = {host: [] for host in simulation.hosts}
    else:
      if not task_states or not timetable:
        raise Exception("initial state must be provided")
      self._task_states = task_states
      self._timetable = timetable

  def copy(self):
    """
    Return a deep (enough) copy of a state.

    Timesheet tuples aren't actually copied, but they shouldn't be modified anyway.

    Note:
      Exists purely for optimization. copy.deepcopy is just abysmally slow.
    """
    # manual copy of initial state
    #   copy.deepcopy is slow as hell
    task_states = {task: dict(state) for (task, state) in self._task_states.items()}
    timetable = {host: list(timesheet) for (host, timesheet) in self._timetable.items()}
    return SchedulerState(task_states=task_states, timetable=timetable)

  @property
  def task_states(self):
    """
    Get current task states as a dict.

    Layout: a dict {Task: {"ect": float, "host": Host}}
    """
    return self._task_states

  @property
  def timetable(self):
    """
    Get a timesheets dict.

    Layout: a dict {Host: [(Task, start, finish)...]}
    """
    return self._timetable

  @property
  def schedule(self):
    """
    Get a schedule from a current timetable.

    Layout: a dict {Host: [Task...]}
    """
    return {host: [task for (task, _, _) in timesheet] for (host, timesheet) in self._timetable.items()}

  @property
  def max_time(self):
    """
    Get a finish time of a last scheduled task in a state.

    Returns NaN if no tasks are scheduled.
    """
    finish_times = [s["ect"] for s in self._task_states.values() if numpy.isfinite(s["ect"])]
    return numpy.nan if not finish_times else max(finish_times)

  def update(self, csimdag.Task task, cplatform.Host host, int pos, double start, double finish):
    """
    Update timetable for a given host.

    Note:
      Doesn't perform any validation for now, can produce overlapping timesheets if used carelessly.
      Checks can be costly.


    Args:
      task: task to schedule on a host
      host: host considered
      pos: insertion position
      start: task start time
      finish: task finish time
    """
    # update task state
    cdef dict task_state = self._task_states[task]
    cdef list timesheet = self._timetable[host]
    task_state["ect"] = finish
    task_state["host"] = host
    # update timesheet
    timesheet.insert(pos, (task, start, finish))


cdef class MinSelector(object):
  """
  Little aux class to select minimum over a loop without storing all the results.

  Doesn't seem to benefit a lot from cython, but why not.
  """
  cdef tuple best_key
  cdef object best_value

  def __init__(self):
    self.best_key = None
    self.best_value = None

  cpdef update(self, tuple key, object value):
    """
    Update selector state.

    If given key compares less then the stored key, overwrites the latter as a new best.

    Args:
      key: key that is minimized
      value: associated value
    """
    if self.best_key is None or key < self.best_key:
      self.best_key = key
      self.best_value = value

  @property
  def key(self):
    """
    Get current best key.
    """
    return self.best_key

  @property
  def value(self):
    """
    Get current best value.
    """
    return self.best_value


cpdef try_schedule_boundary_task(csimdag.Task task, PlatformModel platform_model, SchedulerState state):
  cdef str ROOT_NAME = "root"
  cdef str END_NAME = "end"
  cdef bytes MASTER_NAME = b"master"
  if (task.name != ROOT_NAME and task.name != END_NAME) or task.amount > 0:
    return False
  cdef common.intptr master_host = <common.intptr>cplatform.sg_host_by_name(MASTER_NAME)
  if not master_host:
    return False
  cdef double finish, start
  for host, timesheet in state.timetable.items():
    if host.native == master_host:
      finish = start = timesheet[-1][2] if timesheet else 0
      state.update(task, host, len(timesheet), start, finish)
      break
  else:
    return False
  return True

cpdef is_master_host(cplatform.Host host):
  cdef str MASTER_NAME = "master"
  return host.name == MASTER_NAME

def heft_order(object nxgraph, PlatformModel platform_model):
  """
  Order task according to HEFT ranku.

  Args:
    nxgraph: full task graph as networkx.DiGraph
    platform_model: cscheduling.PlatformModel instance

  Returns:
    a list of tasks in a HEFT order
  """
  cdef double mean_speed = platform_model.mean_speed
  cdef double mean_bandwidth = platform_model.mean_bandwidth
  cdef double mean_latency = platform_model.mean_latency
  cdef dict task_ranku = {}
  for idx, task in enumerate(list(reversed(list(networkx.topological_sort(nxgraph))))):
    ecomt_and_rank = [
      task_ranku[child] + (edge["weight"] / mean_bandwidth + mean_latency)
      for child, edge in nxgraph[task].items()
    ] or [0]
    task_ranku[task] = task.amount / mean_speed + max(ecomt_and_rank) + 1
  # use node name as an additional sort condition to deal with zero-weight tasks (e.g. root)
  return sorted(nxgraph.nodes(), key=lambda node: (task_ranku[node], node.name), reverse=True)


cpdef heft_schedule(object nxgraph, PlatformModel platform_model, SchedulerState state, list ordered_tasks):
  """
  Build a HEFT schedule for a given state.
  Implemented as a separate function to be used in lookahead scheduler.

  Note:
    This function actually modifies the passed SchedulerState, take care. Clone it manually if required.

  Args:
    nxgraph: full task graph as networkx.DiGraph
    platform_model: cscheduling.PlatformModel object
    state: cscheduling.SchedulerState object
    ordered_tasks: tasks in a HEFT order

  Returns:
    modified scheduler state
  """
  cdef MinSelector current_min
  cdef int pos
  cdef cplatform.Host host
  cdef double est, eet, start, finish
  for task in ordered_tasks:
    if try_schedule_boundary_task(task, platform_model, state):
      continue
    current_min = MinSelector()
    for host, timesheet in state.timetable.items():
      if is_master_host(host):
        continue
      est = platform_model.est(host, dict(nxgraph.pred[task]), state)
      eet = platform_model.eet(task, host)
      pos, start, finish = timesheet_insertion(timesheet, est, eet)
      # strange key order to ensure stable sorting:
      #  first sort by ECT (as HEFT requires)
      #  if equal - sort by host speed
      #  if equal - sort by host name (guaranteed to be unique)
      current_min.update((finish, host.speed, host.name), (host, pos, start, finish))
    host, pos, start, finish = current_min.value
    #print(task.name, host.name, pos, est, start, finish)
    state.update(task, host, pos, start, finish)
  return state


def schedulable_order(object nxgraph, dict ranking):
  """
  Give an valid topological order that attempts to prioritize task by given ranking.

  Higher rank values are considered to have higher priority.

  Useful utility to implement a lot of scheduling algorithms (PEFT and more) when a ranking
  function doesn't guarantee to preserve topological sort.

  Args:
    nxgraph: workflow as a networkx.DiGraph object
    ranking: dict of ranking values, layout is {cplatform.Task: float}

  Returns:
    a list of tasks in a topological order
  """
  cdef object state = networkx.DiGraph(nxgraph)
  cdef dict succ = dict(state.succ)
  cdef dict temp_pred = dict(state.pred)
  cdef dict pred = {node : dict(parents) for (node, parents) in temp_pred.items()}
  # as always, use dual key to achieve deterministic sort on equal rank values
  sorter = lambda node: (ranking[node], node.name)
  # extract graph root(s)
  ready_nodes = sorted([node for (node, parents) in pred.items() if not parents], key=sorter)
  order = []
  while ready_nodes:
    scheduled = ready_nodes.pop()
    order.append(scheduled)
    for child in succ[scheduled]:
      child_active_parents = pred[child]
      del child_active_parents[scheduled]
      if not child_active_parents:
        ready_nodes.append(child)
      ready_nodes = sorted(ready_nodes, key=sorter)

  assert len(order) == len(nxgraph)
  return order


cpdef timesheet_insertion(list timesheet, double est, double eet):
  """
  Evaluate a earliest possible insertion into a given timesheet.

  Args:
    timesheet: list of scheduled tasks in a form (Task, start, finish)
    est: new task earliest start time
    eet: new task execution time

  Returns:
    a tuple (insert_index, start, finish)
  """
  # implementation may look a bit ugly, but it's for performance reasons
  cdef int insert_index = len(timesheet)
  cdef double start_time = timesheet[-1][2] if timesheet else 0
  cdef double slot_start
  cdef double slot_end
  cdef double slot
  cdef tuple insertion = (None, 0, 0)
  cdef tuple t1
  cdef tuple t2

  if timesheet:
    for idx in range(len(timesheet)):
      t1 = timesheet[idx - 1] if idx else insertion
      t2 = timesheet[idx]
      slot_end = t2[1]
      slot_start = t1[2]
      slot = slot_end - max(slot_start, est)
      if slot > eet:
        insert_index = idx
        start_time = t1[2]
        break

  start_time = max(start_time, est)
  return (insert_index, start_time, (start_time + eet))
