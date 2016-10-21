# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

import abc
from ..six import with_metaclass
from .. import csimdag

class Scheduler(with_metaclass(abc.ABCMeta)):
  def __init__(self, simulation):
    self._simulation = simulation

  @abc.abstractmethod
  def run(self):
    raise NotImplementedError()

  def _check_done(self):
    unfinished = self._simulation.all_tasks.by_prop("state", csimdag.TASK_STATE_DONE, True)
    if any(unfinished):
      raise Exception("some tasks are not finished by the end of simulation: {}".format([t.name for t in unfinished]))


class StaticScheduler(Scheduler):
  def __init__(self, simulation):
    self._simulation = simulation

  def run(self):
    self._schedule(self._simulation)
    unscheduled = self._simulation.tasks.by_func(lambda t: t.state not in {csimdag.TASK_STATE_RUNNABLE, csimdag.TASK_STATE_SCHEDULED})
    if any(unscheduled):
      raise Exception("some tasks are left unscheduled by static algorithm: {}".format([t.name for t in unscheduled]))
    self._simulation.simulate()
    self._check_done()

  @abc.abstractmethod
  def _schedule(self, simulation):
    raise NotImplementedError()


class DynamicScheduler(Scheduler):
  def run(self):
    self._prepare(self._simulation)
    for t in self._simulation.tasks:
      t.watch(csimdag.TASK_STATE_DONE)
    # a bit ugly kludge - cannot just pass an empty list there, needs to be a _TaskList
    self._schedule(self._simulation, self._simulation.all_tasks.by_func(lambda t: False))
    changed = self._simulation.simulate()
    while changed:
      self._schedule(self._simulation, changed)
      changed = self._simulation.simulate()
    self._check_done()

  @abc.abstractmethod
  def _prepare(self, simulation):
    raise NotImplementedError()

  @abc.abstractmethod
  def _schedule(self, simulation, changed):
    raise NotImplementedError()
