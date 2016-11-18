# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from copy import deepcopy
import numpy as np


class Taskflow(object):
  """
  Complementary class for task graph representation.

  Attributes:
  tasks - List of tasks ids. Also uses as the connection matrix header
  matrix - Connection matrix
  complexities - Tasks computation cost dictionary
  """

  def __init__(self, simdag_tasks):
    self.tasks, self.matrix = self._construct_connection_matrix(simdag_tasks)
    self._complexities = {task.name: task.amount for task in simdag_tasks}

  def _construct_connection_matrix(self, tasks):
    """
    Create the connection matrix from the set of tasks with links between them.
    """
    length = len(tasks)
    header = [task.name for task in tasks]
    matrix = []
    for task in tasks:
      matrix_line = [np.nan] * length
      for child in task.children:
        matrix_line[header.index(child.children[0].name)] = child.amount
      matrix.append(matrix_line)
    return (header, np.array(matrix))

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
