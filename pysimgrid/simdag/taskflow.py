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


from copy import deepcopy
import numpy as np
from .. import csimdag
from .. import cplatform


class Taskflow(object):
  """
  Complementary class for task graph representation.

  Attributes:
  tasks - List of tasks ids. Also uses as the connection matrix header
  matrix - Connection matrix
  complexities - Tasks computation cost dictionary
  """

  def __init__(self):
    self.TRUE_ROOT = '<TRUE_ROOT>'
    self.TRUE_END = '<TRUE_END>'

  def from_simulation(self, simulation):
    self.tasks, self.matrix = self._construct_connection_matrix(simulation)
    self._complexities = {task.name: task.amount for task in simulation.tasks}
    if self.TRUE_ROOT in self.tasks:
      self._complexities[self.TRUE_ROOT] = 0
    if self.TRUE_END in self.tasks:
      self._complexities[self.TRUE_END] = 0
    return self

  def from_data(self, tasks, matrix, complexities):
    self.tasks = tasks
    self.matrix = matrix
    self._complexities = complexities
    if self.TRUE_ROOT in self.tasks:
      self._complexities[self.TRUE_ROOT] = 0
    if self.TRUE_END in self.tasks:
      self._complexities[self.TRUE_END] = 0
    return self

  def _construct_connection_matrix(self, simulation):
    """
    Create the connection matrix from the set of tasks with links between them.
    """
    length = len(simulation.tasks)
    header = [task.name for task in simulation.tasks]

    bandwidth_matrix = np.zeros((len(simulation.hosts), len(simulation.hosts)))
    latency_matrix = np.zeros((len(simulation.hosts), len(simulation.hosts)))
    for i, src in enumerate(simulation.hosts):
      for j in range(i+1, len(simulation.hosts)):
        dst = simulation.hosts[j]
        bandwidth_matrix[i,j] = bandwidth_matrix[j,i] = cplatform.route_bandwidth(src, dst)
        latency_matrix[i,j] = latency_matrix[j,i] = cplatform.route_latency(src, dst)

    mean_comm = bandwidth_matrix.sum() / (bandwidth_matrix.size - bandwidth_matrix.shape[0])
    mean_lat = latency_matrix.sum() / (latency_matrix.size - latency_matrix.shape[0])

    matrix = []
    for task in simulation.tasks:
      matrix_line = [np.nan] * length
      for child in task.children:
        if child.kind != csimdag.TaskKind.TASK_KIND_COMM_E2E:
          continue
        # not stricly correct, but fast approximation of mean ECOMT
        matrix_line[header.index(child.children[0].name)] = child.amount / mean_comm + mean_lat
      matrix.append(matrix_line)
    numpy_matrix = np.array(matrix)

    # Add pseudo-root and -end tasks if the taskflow has several roots/ends
    roots = np.where(np.isnan(numpy_matrix).all(axis=0))[0]
    ends = np.where(np.isnan(numpy_matrix).all(axis=1))[0]
    if len(roots) > 1:
      numpy_matrix = np.r_[
        np.array([[(0. if i in roots else np.nan) for i in range(len(header))]]),
        numpy_matrix
      ]
      header = [self.TRUE_ROOT] + header
      numpy_matrix = np.c_[
        np.array([[np.nan] * len(header)]),
        numpy_matrix
      ]
    if len(ends) > 1:
      numpy_matrix = np.c_[
        numpy_matrix,
        np.array([[0. if i in ends else np.nan] for i in range(len(header))])
      ]
      header = header + [self.TRUE_END]
      numpy_matrix = np.r_[
        numpy_matrix,
        np.array([[np.nan] * len(header)])
      ]

    return (header, numpy_matrix)

  @property
  def complexities(self):
    """
    Return the computation cost dictionary.
    """
    return self._complexities

  @property
  def root(self):
    """
    Return the root task.
    Every graph contains only one root task.

    np.where returns a 1-element tuple with a 1-element array with result,
    so the np.where(...)[0][0] returns the index of the root task
    """
    return self.tasks[np.where(np.isnan(self.matrix).all(axis=0))[0][0]]

  @property
  def end(self):
    """
    Return the end task.
    Every graph contains only one end task.

    np.where returns a 1-element tuple with a 1-element array with result,
    so the np.where(...)[0][0] returns the index of the end task
    """
    return self.tasks[np.where(np.isnan(self.matrix).all(axis=1))[0][0]]

  def get_parents(self, task_id):
    """
    Get the parents list of the task_id task.
    """
    return [
      self.tasks[i]
      for (i, id_) in enumerate([
        line[self.tasks.index(task_id)]
        for line in self.matrix
      ])
      if not np.isnan(id_)
    ]

  def get_children(self, task_id):
    """
    Get the children list of the task_id task.
    """
    return [
      self.tasks[i]
      for (i, id_) in enumerate(self.matrix[self.tasks.index(task_id)])
      if not np.isnan(id_)
    ]

  def topological_order(self, reverse=False):
    """
    Return the list of all tasks in topological order.
    """
    matrix = deepcopy(self.matrix)
    ordered_tasks = []
    tasks_count = len(self.tasks)
    while len(ordered_tasks) < tasks_count:
      indicies = [i for (i, line) in enumerate(matrix) if all(np.isnan(line))]
      for i in indicies:
        task = self.tasks[i]
        if task not in ordered_tasks:
          ordered_tasks.append(task)
        matrix[i] = [np.nan] * tasks_count
        for line in matrix:
          line[i] = np.nan
    if not reverse:
      ordered_tasks.reverse()
    return ordered_tasks
