# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# along with this library.  If not, see <http://www.gnu.org/licenses/>.
#

"""
SimGrid/SimDAG API wrappers.

Minimal usability improvements over the actual C API,
for actual simulation it is better to use :class:`pysimgrid.simdag.Simulation`.
"""

cimport common
cimport xbt
cimport cplatform
cimport csimdag

import os

cpdef enum TaskFileFormat:
  TASK_FILE_FORMAT_UNKNOWN = 0
  TASK_FILE_FORMAT_AUTO = 0
  TASK_FILE_FORMAT_DAX = 1
  TASK_FILE_FORMAT_DOT = 2


cpdef enum TaskState:
  TASK_STATE_NOT_SCHEDULED = csimdag.SD_NOT_SCHEDULED
  TASK_STATE_SCHEDULABLE = csimdag.SD_SCHEDULABLE
  TASK_STATE_SCHEDULED = csimdag.SD_SCHEDULED
  TASK_STATE_RUNNABLE = csimdag.SD_RUNNABLE
  TASK_STATE_RUNNING = csimdag.SD_RUNNING
  TASK_STATE_DONE = csimdag.SD_DONE
  TASK_STATE_FAILED = csimdag.SD_FAILED


cpdef enum TaskKind:
  TASK_KIND_NOT_TYPED = csimdag.SD_TASK_NOT_TYPED
  TASK_KIND_COMM_E2E = csimdag.SD_TASK_COMM_E2E
  TASK_KIND_COMP_SEQ = csimdag.SD_TASK_COMP_SEQ
  TASK_KIND_COMP_PAR_AMDAHL = csimdag.SD_TASK_COMP_PAR_AMDAHL
  TASK_KIND_COMM_PAR_MXN_1D_BLOCK = csimdag.SD_TASK_COMM_PAR_MXN_1D_BLOCK

# constants need to be renamed
LIBRARY_VERSION_MAJOR = csimdag.SIMGRID_VERSION_MAJOR
LIBRARY_VERSION_MINOR = csimdag.SIMGRID_VERSION_MINOR
LIBRARY_VERSION_PATCH = csimdag.SIMGRID_VERSION_PATCH
LIBRARY_VERSION_STRING = csimdag.SIMGRID_VERSION_STRING

cdef list _tasks_from_dynar(xbt.xbt_dynar_t dynar, bint dispose_container=True):
  """
  Extract tasks from dynar.

  Weird implementation is intentional (elementwise-copy instead of 'to_array') so original dynar is not always destroyed.
  """
  cdef list result = []
  cdef void* element = NULL
  cdef int count = 0
  if dynar:
    count = xbt.xbt_dynar_length(dynar)
    for idx in range(count):
      xbt.xbt_dynar_get_cpy(dynar, idx, &element)
      result.append(Task.wrap(<csimdag.SD_task_t>element))
    if dispose_container:
      xbt.xbt_dynar_free_container(&dynar)
  return result

def initialize():
  """
  Initialize SimDAG.
  Must be called before any other SimDAG functions.

  Note:
    Doesn't pass any command-line configuration. Use 'config' function if required.
  """
  cdef int argc = 1
  cdef int* argcp = &argc
  cdef char* argv0 = b"(python bindings)"
  csimdag.SD_init(argcp, &argv0)


def config(key, value):
  """
  Set SimGrid configuration parameter.
  """
  cdef bytes ckey = common.utf8_string(key)
  cdef bytes cvalue = common.utf8_string(value)
  csimdag.SD_config(ckey, cvalue)


def log_config(config_string):
  """
  Set SimGrid XBT logging parameter.
  """
  cdef bytes cvalue = common.utf8_string(config_string)
  xbt.xbt_log_control_set(cvalue)


def load_platform(path):
  """
  Load simulated cplatform definition.
  """
  cdef bytes utf8path = common.utf8_string(path)
  csimdag.SD_create_environment(utf8path)
  cdef int hosts_count = cplatform.sg_host_count()
  if hosts_count:
    return cplatform.Host.wrap_batch(cplatform.sg_host_list(), hosts_count)
  return []


def load_tasks(path, TaskFileFormat format = TASK_FILE_FORMAT_AUTO):
  """
  Load tasks definition.

  Note:
    SimDAG doesn't provide 'static' API to access all active tasks (unlike with hosts).
    To process tasks you must store return value of this function.
  """
  cdef list result = []
  cdef bytes utf8path = common.utf8_string(path)
  # if format is AUTO, try to guess actual format from file extension
  if format == TASK_FILE_FORMAT_AUTO:
    _, ext = os.path.splitext(utf8path)
    format = {
      b".xml": TASK_FILE_FORMAT_DAX,
      b".dax": TASK_FILE_FORMAT_DAX,
      b".dot": TASK_FILE_FORMAT_DOT,
    }.get(ext.lower(), TASK_FILE_FORMAT_UNKNOWN)
  # actually load it
  cdef xbt.xbt_dynar_t loaded_tasks = NULL
  if format == TASK_FILE_FORMAT_DAX:
    loaded_tasks = csimdag.SD_daxload(utf8path)
  elif format == TASK_FILE_FORMAT_DOT:
    loaded_tasks = csimdag.SD_dotload(utf8path)
  else:
    raise Exception("Unable to determine task file format")
  if loaded_tasks:
    result = _tasks_from_dynar(loaded_tasks)
  return result


def simulate(double how_long=-1.):
  """
  Run the simgrid simulation until one of the following happens:

  * how_long time limit expires (if passed and positive)
  * watchpoint is reached (some task changed state)
  * simulation ends

  Returns the list of changed tasks.
  """
  cdef xbt.xbt_dynar_t changed_tasks = csimdag.SD_simulate(how_long)
  return _tasks_from_dynar(changed_tasks, False)


def get_clock():
  """
  Get current simulation clock.
  """
  return csimdag.SD_get_clock()


def add_dependency(csimdag.Task src, csimdag.Task dst):
  """
  Add dependency between given tasks, if not already exists.
  """
  cdef bytes utf8name = common.utf8_string("scheduled-after")
  parents = set()
  for parent in dst.parents:
    parents.add(parent.name)
  if src.name not in parents:
    csimdag.SD_task_dependency_add(utf8name, NULL, src.impl, dst.impl)


def exit():
  """
  Finalize simulator operation.

  Disposes SimDAG static structures and our pythonic sugar.
  """
  csimdag.SD_exit()


cdef class Task:
  """
  SimDAG task representation.
  """
  @staticmethod
  cdef Task wrap(csimdag.SD_task_t impl):
    """
    Wrap native task handle.
    """
    cdef Task task = Task()
    task.impl = impl
    return task

  @staticmethod
  cdef list wrap_batch(csimdag.SD_task_t* tasks, int count):
    """
    Wrap native tasks array.
    """
    cdef list result = []
    for idx in range(count):
      result.append(Task.wrap(tasks[idx]))
    return result

  def watch(self, TaskState state):
    """
    Instruct simulation engine to stop simulation when this tasks transitions to a given state.
    """
    self.__check_impl()
    csimdag.SD_task_watch(self.impl, <e_SD_task_state_t>state)

  def unwatch(self, TaskState state):
    """
    Remove watchpoint (see 'watch' for explanation).
    """
    self.__check_impl()
    csimdag.SD_task_unwatch(self.impl, <e_SD_task_state_t>state)

  def schedule(self, cplatform.Host host not None):
    """
    Schedule this task to a given host.
    """
    self.__check_impl()
    if not host.impl:
      raise RuntimeError("host instance is invalid")
    csimdag.SD_task_schedulev(self.impl, 1, &host.impl)

  def schedule_after(self, cplatform.Host host not None, csimdag.Task predecessor not None):
    """
    Schedule this task to a given host after the predecessor task.
    """
    self.__check_impl()
    if not host.impl:
      raise RuntimeError("host instance is invalid")
    parents = set()
    for comm in self.parents:
      parents.add(comm.parents[0].name)
    cdef bytes utf8name = common.utf8_string("scheduled-after")
    if predecessor.name not in parents and predecessor.state < TaskState.TASK_STATE_DONE:
      csimdag.SD_task_dependency_add(utf8name, NULL, predecessor.impl, self.impl)
    csimdag.SD_task_schedulev(self.impl, 1, &host.impl)

  def get_eet(self, cplatform.Host host not None):
    """
    Estimate task execution time on a given host.

    Note:
      Valid only for computational tasks
    """
    self.__check_impl()
    if not host.impl:
      raise RuntimeError("host instance is invalid")
    if self.kind == TASK_KIND_NOT_TYPED:
      raise Exception("cannot estimate execution time for untyped task")
    if self.kind == TASK_KIND_COMM_E2E:
      raise Exception("cannot estimate execution time for communication task")
    return self.amount / host.speed

  def get_ecomt(self, cplatform.Host src not None, cplatform.Host dst not None):
    """
    Estimate communication between given hosts.

    Note:
      Valid only for communication tasks
    """
    self.__check_impl()
    if (not src.impl) or (not dst.impl):
      raise RuntimeError("host instance is invalid")
    if self.kind == TASK_KIND_NOT_TYPED:
      raise Exception("cannot estimate communication time for untyped task")
    if self.kind != TASK_KIND_COMM_E2E:
      raise Exception("cannot estimate communication time for computational task")
    return cplatform.SD_route_get_latency(src.impl, dst.impl) + self.amount / cplatform.SD_route_get_bandwidth(src.impl, dst.impl)

  def dump(self):
    """
    Dump task state to stdout in SimGrid format.
    """
    self.__check_impl()
    csimdag.SD_task_dump(self.impl)

  @property
  def native(self):
    """
    Get/set task native handle as intptr_t.

    Warning:
      Be very careful with setting this property, wrong values will lead to SEGFAULT and no amout of try/excepts will save you.
    """
    return <common.intptr>self.impl

  @native.setter
  def native(self, common.intptr value):
    self.impl = <csimdag.SD_task_t>value

  @property
  def name(self):
    """
    Get/set task name.
    """
    self.__check_impl()
    cdef const char* cname = csimdag.SD_task_get_name(self.impl)
    return cname.decode("utf-8")

  @name.setter
  def name(self, name):
    self.__check_impl()
    cdef bytes utf8name = common.utf8_string(name)
    csimdag.SD_task_set_name(self.impl, utf8name)

  @property
  def amount(self):
    """
    Get task 'size': flops for computational tasks, bytes for transfer tasks.
    Real value is replaced by a possibly inaccurate estimate if set.
    """
    self.__check_impl()
    if self.amount_estimate != -1:
        return self.amount_estimate
    else:
        return csimdag.SD_task_get_amount(self.impl)

  @property
  def amount_estimate(self):
    """
    Get task amount estimate (possibly inaccurate)
    """
    self.__check_impl()
    return self.amount_estimate

  @amount_estimate.setter
  def amount_estimate(self, amount_estimate):
    self.__check_impl()
    self.amount_estimate = amount_estimate

  @property
  def amount_real(self):
    """
    Get real (accurate) task amount
    """
    self.__check_impl()
    return csimdag.SD_task_get_amount(self.impl)

  @property
  def kind(self):
    """
    Get task kind (see TaskKind enum).
    """
    self.__check_impl()
    cdef csimdag.e_SD_task_kind_t ckind = csimdag.SD_task_get_kind(self.impl)
    return TaskKind(ckind)

  @property
  def state(self):
    """
    Get current task state (see TaskState enum).
    """
    self.__check_impl()
    cdef csimdag.e_SD_task_state_t cstate = csimdag.SD_task_get_state(self.impl)
    return TaskState(cstate)

  @property
  def start_time(self):
    """
    Task start time.

    Note:
      Invalid for tasks that are not started yet.
    """
    self.__check_impl()
    return csimdag.SD_task_get_start_time(self.impl)

  @property
  def finish_time(self):
    """
    Get task finish time.

    Note:
      Invalid for tasks that are not done yet.
    """
    self.__check_impl()
    return csimdag.SD_task_get_finish_time(self.impl)

  @property
  def children(self):
    """
    Get task direct children.

    Note:
      As communication is also represented by tasks, to get child computational tasks you will need to call it recursively.
    """
    self.__check_impl()
    return _tasks_from_dynar(csimdag.SD_task_get_children(self.impl))

  @property
  def parents(self):
    """
    Get task direct children.

    Note:
      As communication is also represented by tasks, to get parent computational tasks you will need to call it recursively.
    """
    self.__check_impl()
    return _tasks_from_dynar(csimdag.SD_task_get_parents(self.impl))

  @property
  def hosts(self):
    """
    Get hosts where tasks is/will be executed.
    """
    self.__check_impl()
    cdef int count = csimdag.SD_task_get_workstation_count(self.impl)
    if count:
      return cplatform.Host.wrap_batch(csimdag.SD_task_get_workstation_list(self.impl), count)
    return []

  @property
  def data(self):
    """
    Get/set user data associated with this host.

    Data can be arbitrary python object, however the most common/robust approach is to store a dict of some properties.
    """
    return self.user_data

  @data.setter
  def data(self, object value):
    self.user_data = value

  @data.deleter
  def data(self):
    self.user_data = None

  def __cinit__(self):
    """
    Basic initialization.
    """
    self.impl = NULL
    self.amount_estimate = -1

  def __check_impl(self):
    """
    Validate the impl pointer.
    """
    if self.impl == NULL:
      raise RuntimeError("Task instance is NULL")
