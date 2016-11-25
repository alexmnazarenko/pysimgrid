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
from .heft import HEFTScheduler


class LookaheadScheduler(StaticScheduler):

  def get_schedule(self, simulation):
    self.taskflow = Taskflow().from_simulation(simulation)
    heft_order = self._get_heft_order()
    raw_schedule = {
      host.name: {
        "speed": host.speed,
        "timesheet": []
      }
      for host in simulation.hosts
    }
    tasks_info = {}
    for task in heft_order:
      parents = self.taskflow.get_parents(task)
      est = max([tasks_info[p]["end_time"] for p in parents if p in tasks_info] + [0])
      hosts_taskinfo = {}
      for host in simulation.hosts:
        temp_schedule = deepcopy(raw_schedule)
        start, end = self._assign_task(
          est,
          self.taskflow.complexities[task],
          raw_schedule[host.name]
        )
        temp_schedule[host.name]['timesheet'].append((
          start,
          end,
          (task, self.taskflow.complexities[task])
        ))
        temp_schedule[host.name]['timesheet'].sort()

        subtaskflow = self._construct_subtaskflow(task)
        subschedule = HEFTScheduler.construct_heft_schedule(
          subtaskflow,
          simulation.hosts,
          temp_schedule,
          task
        )
        end_time = max(
          h['timesheet'][-1][1]
          for h in subschedule.values()
          if h['timesheet']
        )
        hosts_taskinfo[host.name] = {
          'start_task_time': start,
          'end_task_time': end,
          'end_time_total': end_time
        }

      host_to_assign, task_info = min(
        hosts_taskinfo.items(),
        key=lambda x: x[1]['end_time_total']
      )
      raw_schedule[host_to_assign]['timesheet'].append((
        task_info['start_task_time'],
        task_info['end_task_time'],
        (task, self.taskflow.complexities[task])
      ))
      raw_schedule[host_to_assign]['timesheet'].sort()

      tasks_info[task] = {
        "host": host_to_assign,
        "start_time": hosts_taskinfo[host_to_assign]['start_task_time'],
        "end_time": hosts_taskinfo[host_to_assign]['end_task_time']
      }

    schedule = {}
    tasks_map = {t.name: t for t in simulation.tasks}
    for host in simulation.hosts:
      schedule[host] = []
      for elem in raw_schedule[host.name]["timesheet"]:
        task_name = elem[2][0]
        if task_name not in [self.taskflow.TRUE_ROOT, self.taskflow.TRUE_END]:
          schedule[host].append(tasks_map[task_name])

    return schedule

  def _get_heft_order(self):
    ranked_tasks = HEFTScheduler.rankify_tasks(self.taskflow)
    ordered_tasks = HEFTScheduler.order_tasks(ranked_tasks)
    return [t[0] for t in ordered_tasks]

  def _construct_subtaskflow(self, task):
    queue = self.taskflow.get_children(task)
    processed = [task]
    while queue:
      current_task = queue.pop()
      for child in self.taskflow.get_children(current_task):
        if child not in processed and child not in queue:
          queue.append(child)
      if current_task not in processed:
        processed.append(current_task)
    processed.pop(0)
    processed = [self.taskflow.tasks.index(t) for t in processed]
    processed.sort()
    subtree_header = [self.taskflow.tasks[i] for i in processed]
    subtree_matrix = self.taskflow.matrix[np.ix_(processed, processed)]
    subtree_complexities = {
      t: self.taskflow.complexities[t]
      for t in subtree_header
    }
    subtaskflow = Taskflow().from_data(
      subtree_header,
      subtree_matrix,
      subtree_complexities
    )
    return subtaskflow

  def _assign_task(self, est, amount, host_info):
    # Check host gaps
    duration = float(amount) / host_info["speed"]
    pairs = zip(host_info["timesheet"], host_info["timesheet"][1:])
    gaps = [(p[0][1], p[1][0]) for p in pairs if p[0][1] != p[1][0]]
    for gap in gaps:
      start = max(gap[0], est)
      end = gap[1]
      if end < est or duration > end - start:
        continue
      return (start, start + duration)

    # End time of the last host task
    start = (
      host_info["timesheet"][-1][1]
      if len(host_info["timesheet"])
      else 0
    )
    start = max(start, est)

    return (start, start + duration)
