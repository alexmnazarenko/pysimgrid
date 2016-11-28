# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from copy import deepcopy
import numpy as np

from ..scheduler import StaticScheduler
from ..taskflow import Taskflow


class PEFTScheduler(StaticScheduler):

  def _construct_oct(self, simulation, taskflow):
    """
    Construct the optimistic cost table.
    """
    self._oct = np.array(
      [[0.0] * len(simulation.hosts)] * len(simulation.tasks)
    )
    for i, task in enumerate(self._rt_order):
      if task == taskflow.end:
        continue
      task_row = self._oct[i]
      for h, host in enumerate(simulation.hosts):
        current_oct = 0
        for child in taskflow.get_children(task):
          child_row = self._oct[taskflow.tasks.index(child)]
          min_child_time = min(
            cell +
            float(taskflow.complexities[child]) / simulation.hosts[j].speed +
            (taskflow.matrix[taskflow.tasks.index(task), taskflow.tasks.index(child)] if j != h else 0)
            for j, cell in enumerate(child_row)
          )
          if min_child_time > current_oct:
            current_oct = min_child_time
        task_row[h] = current_oct

  def _rankify_tasks(self):
    ranked_tasks = {
      task: np.average(self._oct[i])
      for i, task in enumerate(self._rt_order)
    }
    return ranked_tasks

  def _order_tasks(self, ranked_tasks):
    return sorted(ranked_tasks.items(), key=lambda x: x[1], reverse=True)

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

  def get_schedule(self, simulation):
    taskflow = Taskflow().from_simulation(simulation)
    self._rt_order = taskflow.topological_order(reverse=True)
    self._construct_oct(simulation, taskflow)
    tasks_map = {t.name: t for t in simulation.tasks}
    ranked_tasks = self._rankify_tasks()
    ordered_tasks = self._order_tasks(ranked_tasks)

    hosts = {
      host.name: {
        "speed": host.speed,
        "timesheet": []
      }
      for host in simulation.hosts
    }
    tasks_info = {}
    ready = []
    for task in ordered_tasks:
      if task[0] == taskflow.root:
        ready.append(task)
        break
    processed = []
    while ready:
      task = max(ready, key=lambda x: x[1])

      # Remove the task from ready list
      ready.pop(ready.index(task))

      parents = taskflow.get_parents(task[0])
      est = max([tasks_info[p]["end_time"] for p in parents if p in tasks_info] + [0])
      host, start_time, end_time = self._calc_host_start(
        est,
        taskflow.complexities[task[0]],
        hosts
      )
      tasks_info[task[0]] = {
        "host": host,
        "start_time": start_time,
        "end_time": end_time
      }
      hosts[host]["timesheet"].append((start_time, end_time, task))
      hosts[host]["timesheet"].sort()

      processed.append(task[0])
      processed_names = set([t for t in processed])
      for child in taskflow.get_children(task[0]):
        if set(taskflow.get_parents(child)) - processed_names:
          continue
        for task in ordered_tasks:
          if task[0] == child:
            ready.append(task)
            break

    schedule = {}
    for host in simulation.hosts:
      schedule[host] = []
      for elem in hosts[host.name]["timesheet"]:
        task_name = elem[2][0]
        schedule[host].append(tasks_map[task_name])
    return schedule
