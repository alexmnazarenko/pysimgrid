# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.


from copy import deepcopy

from ..scheduler import StaticScheduler
from ..taskflow import Taskflow


class HEFTScheduler(StaticScheduler):

  def _rankify_tasks(self, taskflow):
    ranked_tasks = {}
    for task in taskflow.topological_order(reverse=True):
      ecomt_and_rank = [
        ranked_tasks[t] + taskflow.matrix[taskflow.tasks.index(task)][taskflow.tasks.index(t)]
        for t in taskflow.get_children(task)
        if t in ranked_tasks
        if not taskflow.matrix[taskflow.tasks.index(task)][taskflow.tasks.index(t)] is False
      ] or [0]
      ranked_tasks[task] = taskflow.complexities[task] + max(ecomt_and_rank)
    return ranked_tasks

  def _order_tasks(self, ranked_tasks):
    return sorted(ranked_tasks.items(), key=lambda x: x[1], reverse=True)

  def _timesheet_gaps(self, timesheet):
    pairs = zip(timesheet, timesheet[1:])
    return [(p[0][1], p[1][0]) for p in pairs if p[0][1] != p[1][0]]

  def _calc_host_start(self, est, amount, hosts):
    e_host_st = []
    for host in hosts:
      # Check host gaps
      duration = float(amount) / hosts[host]["speed"]
      gaps = self._timesheet_gaps(hosts[host]["timesheet"])
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
        e_host_st.append((host, start, start + duration))
    return min(e_host_st, key=lambda x: x[1])

  def get_schedule(self, simulation):
    taskflow = Taskflow(simulation.tasks)
    tasks_map = {t.name: t for t in simulation.tasks}
    ranked_tasks = self._rankify_tasks(taskflow)
    ordered_tasks = self._order_tasks(ranked_tasks)

    hosts = {
      host.name: {
        "speed": host.speed,
        "timesheet": []
      }
      for host in simulation.hosts
    }
    tasks_info = {}
    for task in ordered_tasks:
      parents = taskflow.get_parents(task[0])
      est = max([tasks_info[p]["end_time"] for p in parents if p in tasks_info] + [0])
      host, start_time, end_time = self._calc_host_start(
        est,
        taskflow.complexities[task[0]],
        hosts
      )
      tasks_info[task] = {
        "host": host,
        "start_time": start_time,
        "end_time": end_time
      }
      hosts[host]["timesheet"].append((start_time, end_time, task))
      hosts[host]["timesheet"].sort()

    schedule = {}
    for host in simulation.hosts:
      schedule[host] = []
      for elem in hosts[host.name]["timesheet"]:
        task_name = elem[2][0]
        schedule[host].append(tasks_map[task_name])
    return schedule
