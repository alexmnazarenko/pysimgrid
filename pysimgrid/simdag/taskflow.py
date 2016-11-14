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
      matrix_line = [False] * length
      for child in task.children:
        matrix_line[header.index(child.children[0].name)] = child.amount
      matrix.append(matrix_line)
    return (header, matrix)

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
    """
    # HACK: rewrite to numpy.array
    transposed = [
      [self.matrix[i][j]
      for i in range(len(self.tasks))]
      for j in range(len(self.tasks))
    ]
    for i, line in enumerate(transposed):
      if not any(line):
        return self.tasks[i]

  @property
  def end(self):
    """
    Return the end task.
    Every graph contains only one end task.
    """
    for i, line in enumerate(self.matrix):
      if not any(line):
        return self.tasks[i]

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
      if id_ != False
    ]

  def get_children(self, task_id):
    """
    Get the children list of the task_id task.
    """
    return [
      self.tasks[i]
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
