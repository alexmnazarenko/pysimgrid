# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

import os
import logging
from .. import csimdag

class Simulation(object):
  """
  High-level API for current simulation state.

  * Ensures proper bootstrap/cleanup
  * Provides an improved API for task/host filtering.
  * Ensures that you always use exactly same python instances for SimGrid objects
  ** e.g csimdag.Task.parents returns brand-new Task instances you've never seen before
  ** required for (Task/Host).data to work properly
  """

  _INSTANCE = None
  _DEFAULT_CONFIG = {
    "network/model": "LV08"
  }

  def __init__(self, platform, tasks, config=None):
    self.__platform_src = platform
    self.__tasks_src = tasks
    self.__config = self._DEFAULT_CONFIG
    if config:
      assert isinstance(config, dict)
      self.__config.update(config)
    self.__hosts = None
    self.__tasks = None
    self.__logger = logging.getLogger("pysimgrid.Simulation")
    if not os.path.isfile(self.__platform_src):
      raise IOError("platform definition file {} does not exist".format(self.__platform_src))
    if not os.path.isfile(self.__tasks_src):
      raise IOError("tasks definition file {} does not exist".format(self.__tasks_src))

  def simulate(self, how_long=-1.):
    """
    Run the simgrid simulation until one of the following happens:
    * how_long time limit expires (if passed and positive)
    * watchpoint is reached (some task changed state)
    * simulation ends

    Returns the list of changed tasks.
    """
    changed = csimdag.simulate(how_long)
    changed_ids = [t.native for t in changed]
    return _TaskList([t for t in self.__tasks if t.native in changed_ids])

  def get_task_graph(self):
    import networkx as nx

    free_tasks = self.tasks.by_func(lambda t: not t.parents)
    if len(free_tasks) != 1:
      raise Exception("cannot find DAG root")

    graph = nx.DiGraph()
    graph.add_nodes_from(self.tasks)

    for e in self.all_tasks.by_prop("kind", csimdag.TASK_KIND_COMM_E2E):
      parents, children = e.parents, e.children
      assert len(parents) == 1 and len(children) == 1
      graph.add_edge(parents[0], children[0], weight=e.amount)

    return graph

  @property
  def tasks(self):
    """
    Get all computational tasks.
    """
    return self.all_tasks.by_prop("kind", csimdag.TASK_KIND_COMM_E2E, True)

  @property
  def all_tasks(self):
    """
    Get full task list, including comm tasks.
    """
    return _TaskList(self.__tasks)

  @property
  def hosts(self):
    """
    Get full host list.
    """
    return _InstanceList(self.__hosts)

  @property
  def clock(self):
    return csimdag.get_clock()


  def __enter__(self):
    """
    Context interface implementation.
    """
    if self._INSTANCE is not None:
      raise Exception("Simulation may be used only once per process (SimGrid currently does not support reinitialization)")

    self.__logger.debug("Initialization started")
    csimdag.initialize()

    self.__logger.debug("Setting configuration parameters")
    for k, v in self.__config.items():
      self.__logger.debug("  %s = %s", k, v)
      csimdag.config(k, v)

    self.__logger.debug("Loading platform definition (source: %s)", self.__platform_src)
    self.__hosts = csimdag.load_platform(self.__platform_src)
    self.__logger.debug("Platform loaded, %d hosts", len(self.__hosts))

    self.__logger.debug("Loading task definition (source: %s)", self.__tasks_src)
    tasks = csimdag.load_tasks(self.__tasks_src)
    self.__tasks = [_SimulationTask(t.native, self) for t in tasks]
    comm_tasks_count = len(list(filter(lambda t: t.kind == csimdag.TASK_KIND_COMM_E2E, self.__tasks)))
    self.__logger.debug("Tasks loaded, %d nodes, %d links", len(self.__tasks) - comm_tasks_count, comm_tasks_count)

    self.__logger.info("Simulation initialized")
    self._INSTANCE = self
    return self

  def __exit__(self, *args):
    """
    Context interface implementation.
    """
    self.__logger.info("Finalizing the simulation (clock: %.2f)", self.clock)
    csimdag.exit()
    return False

class _SimulationTask(csimdag.Task):
  """
  Supporting class - wrap csimdag.Task methods that return new Task/Host instances.
  """
  def __init__(self, native, simulation):
    self.native = native
    self._sim = simulation

  @property
  def hosts(self):
    return self.__remap(super(_SimulationTask, self).hosts, self._sim.hosts)

  @property
  def children(self):
    return self.__remap(super(_SimulationTask, self).children, self._sim.all_tasks)

  @property
  def parents(self):
    return self.__remap(super(_SimulationTask, self).parents, self._sim.all_tasks)

  def __remap(self, internal_list, public_list):
    """
    A bit ugly instance remapper. Strange implementation is to preserve original order (it may suddenly matter).
    """
    ids_order = [obj.native for obj in internal_list]
    ids_set = set(ids_order)
    return public_list.by_func(lambda p: p.native in ids_set).sorted(lambda el: ids_order.index(el.native))


class _InstanceList(object):
  """
  Object list wrapper to simplify common filtering.
  """
  def __init__(self, instances):
    self._list = instances

  def by_prop(self, property_name, value, negate=False):
    """
    Select instances by property value.
    """
    if negate:
      return type(self)([el for el in self._list if getattr(el, property_name) != value])
    return type(self)([el for el in self._list if getattr(el, property_name) == value])

  def by_data(self, key, *value):
    """
    Select instances by data attribute.

    If no 'value' arg is passed, condition is obj.data == key,
                                 else it is   obj.data[key] == value
    """
    if len(value) > 1:
      raise Exception("only single value can be passed")
    if value:
      return type(self)([el for el in self._list if el.data.get(key) == value[0]])
    return type(self)([el for el in self._list if el.data == key])

  def by_func(self, func):
    """
    Select instances by custom filter.
    """
    return type(self)([el for el in self._list if func(el)])

  def sorted(self, key, reverse=False):
    """
    Sort instance on custom criterion.
    """
    return type(self)(sorted(self._list, key=key, reverse=reverse))

  def __getitem__(self, arg):
    """
    Sequence interface implementation.
    """
    if isinstance(arg, int):
      return self._list[arg]
    elif isinstance(arg, slice):
      return type(self)(self._list[slice])
    else:
      raise TypeError("unsupported indexer type")

  def __len__(self):
    """
    Sequence interface implementation.
    """
    return len(self._list)

  def __contains__(self, element):
    """
    Sequence interface implementation.
    """
    return element in self._list

  def __iter__(self):
    """
    Sequence interface implementation.
    """
    return iter(self._list)

  def __str__(self):
    """
    Sequence interface implementation.
    """
    return str(self._list)

class _TaskList(_InstanceList):
  """
  Task list wrapper to simplify common filtering even more.
  """
  def __getitem__(self, arg):
    """
    Sequence interface implementation.
    """
    if isinstance(arg, csimdag.TaskState):
      return self.by_prop("state", arg)
    elif isinstance(arg, csimdag.TaskKind):
      return self.by_prop("kind", arg)
    elif isinstance(arg, int):
      return self._list[arg]
    elif isinstance(arg, slice):
      return type(self)(self._list[slice])
    else:
      raise TypeError("unsupported indexer type")
