# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from numpy import average
from collections import deque
from itertools import chain

from ..scheduler import StaticScheduler
from ..taskflow import Taskflow


class HCPTScheduler(StaticScheduler):

  def get_schedule(self, simulation):
    taskflow = Taskflow(simulation.tasks)
    tasks = taskflow.tasks

    # Average execution cost
    aec = {}
    for task in tasks:
      cost = taskflow.complexities[task]
      task_aec = average([
        float(cost) / host.speed
        for host in simulation.hosts
      ])
      aec[task] = task_aec

    # Average earliest start time
    aest = {}
    aest[taskflow.root] = 0
    ordered_tasks = taskflow.topological_order()
    for task_id in ordered_tasks:
      parents = taskflow.get_parents(task_id)
      if not parents:
        aest[task_id] = 0
        continue
      aest[task_id] = max([
        aest[p] + aec[p] + float(taskflow.matrix[tasks.index(p), tasks.index(task_id)])
        for p in parents
      ])

    # Average latest start time
    alst = {}
    alst[taskflow.end] = aest[taskflow.end]
    ordered_tasks.reverse()
    for task_id in ordered_tasks:
      children = taskflow.get_children(task_id)
      if not children:
        alst[task_id] = aest[task_id]
        continue
      alst[task_id] = min([
        alst[c] - float(taskflow.matrix[tasks.index(task_id), tasks.index(c)])
        for c in children
      ]) - aec[task_id]

    # All nodes in the critical path must have AEST=ALST
    critical_path = sorted(
      set(aest.items()) & set(alst.items()),
      key=lambda x: x[1]
    )
    critical_path.reverse()

    stack = deque([elem[0] for elem in critical_path])
    queue = deque()
    while len(stack):
      task_id = stack.pop()
      parents = taskflow.get_parents(task_id)
      untracked_parents = set(parents) - set(queue)
      if untracked_parents:
        stack.append(task_id)
        for parent in untracked_parents:
          stack.append(parent)
      else:
        queue.append(task_id)

    # Schedule has a format: (Task name, Start time, End time)
    schedule = {host: [] for host in simulation.hosts}
    ids_tasks = {task.name: task for task in simulation.tasks}
    while len(queue):
      task_id = queue.popleft()
      parents = taskflow.get_parents(task_id)
      parents_end = [
        elem[2] + taskflow.matrix[taskflow.tasks.index(elem[0].name), taskflow.tasks.index(task_id)]
        for elem in chain.from_iterable(schedule.values())
        if elem[0].name in parents
      ]
      hosts_eeft = {}
      for host in simulation.hosts:
        host_end = [schedule[host][-1][2]] if len(schedule[host]) else [0.0]
        eeft = (
          min(parents_end + host_end) +
          float(taskflow.complexities[task_id]) / host.speed
        )
        hosts_eeft[host] = eeft
      host_to_assign, time = min(hosts_eeft.items(), key=lambda e: e[1])
      schedule[host_to_assign].append((
        ids_tasks[task_id],
        time - float(taskflow.complexities[task_id]) / host_to_assign.speed,
        time
      ))
    for host in schedule:
      schedule[host] = [elem[0] for elem in schedule[host]]
    return schedule
