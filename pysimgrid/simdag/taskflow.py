# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from copy import deepcopy

class Taskflow(object):
  """
  Complementary class for task graph representation.

  Attributes:
  tasks - List of tasks ids. Also uses as the connection matrix header
  matrix - Connection matrix
  complexities - Tasks complexities dictionary
  """

  def __init__(self, simdag_tasks):
    self.tasks, self.matrix = self._construct_connection_matrix(simdag_tasks)
    self.complexities = self._get_tasks_complexity(simdag_tasks)

  def _construct_connection_matrix(self, tasks):
    """
    Create the connection matrix from the set of tasks with links between them.
    """
    length = len(tasks)
    header = [task.native for task in tasks]
    matrix = []
    for task in tasks:
      matrix_line = [False] * length
      for child in task.children:
        matrix_line[header.index(child.children[0].native)] = child.amount
      matrix.append(matrix_line)
    return (header, matrix)

  def _get_tasks_complexity(self, tasks):
    return {task.native: task.amount for task in tasks}

  def get_parents(self, task_id):
    """
    Get the list of parents of the task_id task.
    """
    return [
      id_
      for (i, id_) in enumerate([
        line[self.tasks.index(task_id)]
        for line in self.matrix
      ])
      if id_ != False
    ]

  def get_children(self, task_id):
    """
    Get the list of children of the task_id task.
    """
    return [
      id_
      for (i, id_) in enumerate(self.matrix[self.tasks.index(task_id)])
      if id_ != False
    ]

  def topological_order(self, reverse=False):
    """
    Return the list of all tasks ordered topologically.
    """
    matrix = deepcopy(self.matrix)
    ordered_tasks = []
    tasks_count = len(self.tasks)
    while len(ordered_tasks) < tasks_count:
      indicies = [i for (i, line) in enumerate(matrix) if not any(line)]
      for i in indicies:
        task = self.tasks[i]
        if task not in ordered_tasks:
          ordered_tasks.append(task)
        matrix[i] = [False] * tasks_count
        for line in matrix:
          line[i] = False
    if not reverse:
      ordered_tasks.reverse()
    return ordered_tasks
