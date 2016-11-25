# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from collections import deque
from itertools import chain
import numpy as np

from ..scheduler import StaticScheduler
from ..taskflow import Taskflow


class HCPTScheduler(StaticScheduler):

  def get_schedule(self, simulation):
    taskflow = Taskflow().from_simulation(simulation)
    tasks = taskflow.tasks

    # Average execution cost
    aec = {}
    for task in tasks:
      cost = taskflow.complexities[task]
      task_aec = np.average([
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
      [(t, aest[t]) for t in aest if np.isclose(aest[t], alst[t])],
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

    hosts = {
      host.name: {
        "speed": host.speed,
        "timesheet": []
      }
      for host in simulation.hosts
    }

    while len(queue):
      task_id = queue.popleft()
      parents = taskflow.get_parents(task_id)
      parents_end = [
        elem[2] + taskflow.matrix[taskflow.tasks.index(elem[0].name), taskflow.tasks.index(task_id)]
        for elem in chain.from_iterable(schedule.values())
        if elem[0].name in parents
      ]
      est = min(parents_end or [0])
      host, start_time, end_time = self._calc_host_start(
        est,
        taskflow.complexities[task_id],
        hosts
      )
      hosts[host]["timesheet"].append((start_time, end_time, task_id))

    for host in simulation.hosts:
      schedule[host] = []
      for elem in hosts[host.name]["timesheet"]:
        task_name = elem[2]
        if task_name not in [taskflow.TRUE_ROOT, taskflow.TRUE_END]:
          schedule[host].append(ids_tasks[task_name])

    return schedule

  def _calc_host_start(self, est, amount, hosts):
    e_host_st = []
    for host in hosts:
      # Check host gaps
      duration = float(amount) / hosts[host]["speed"]
      pairs = zip(hosts[host]["timesheet"], hosts[host]["timesheet"][1:])
      gaps = [(p[0][1], p[1][0]) for p in pairs if p[0][1] != p[1][0]]
      for gap in gaps:
        start = max(gap[0], est)
        end = gap[1]
        if end < est or duration > end - start:
          continue
        e_host_st.append((host, start, start + duration))
        break
      if host not in e_host_st:
        # End time of the last host task
        start = (
          hosts[host]["timesheet"][-1][1]
          if len(hosts[host]["timesheet"])
          else 0
        )
        start = max(start, est)
        e_host_st.append((host, start, start + duration))
    return min(e_host_st, key=lambda x: x[2])