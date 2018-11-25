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

cimport cplatform
cimport common


def host_by_name(name):
  """
  Get a host instance by name.
  """
  cdef bytes utf8name = common.utf8_string(name)
  cdef cplatform.sg_host_t host = cplatform.sg_host_by_name(utf8name)
  if not host:
    raise Exception(u"no host '{}' found".format(utf8name.decode("utf-8")))
  return Host.wrap(host)


def get_hosts():
  """
  Get all hosts in a platform as a list.
  """
  cdef int hosts_count = cplatform.sg_host_count()
  if hosts_count:
    return Host.wrap_batch(cplatform.sg_host_list(), hosts_count)
  return []


def route(Host src not None, Host dst not None):
  """
  Get list of link names in a route.
  """
  if (not src.impl) or (not dst.impl):
    raise Exception("Cannot build route, one of hosts is invalid")
  cdef xbt.xbt_dynar_t links = xbt.xbt_dynar_new(sizeof(void*), NULL)
  cdef void* element = NULL
  cdef list result = []
  cplatform.sg_host_route(src.impl, dst.impl, links)
  links_count = xbt.xbt_dynar_length(links)
  for idx in range(links_count):
    xbt.xbt_dynar_get_cpy(links, idx, &element)
    result.append(cplatform.sg_link_name(element).decode("utf-8"))
  xbt.xbt_dynar_free_container(&links)
  return result


def route_latency(Host src not None, Host dst not None):
  """
  Get route total latency.
  """
  if (not src.impl) or (not dst.impl):
    raise Exception("Cannot build route, one of hosts is invalid")
  return cplatform.SD_route_get_latency(src.impl, dst.impl)


def route_bandwidth(Host src not None, Host dst not None):
  """
  Get route bandwidth.
  """
  if (not src.impl) or (not dst.impl):
    raise Exception("Cannot build route, one of hosts is invalid")
  return cplatform.SD_route_get_bandwidth(src.impl, dst.impl)


cdef class Host:
  """
  Representation of a platform's host.

  See declaration in cplatform.pxd.
  """

  @staticmethod
  cdef Host wrap(cplatform.sg_host_t impl):
    """
    Wrap a native host handle.
    """
    cdef Host host = Host()
    host.impl = impl
    return host

  @staticmethod
  cdef list wrap_batch(sg_host_t* hosts, int count):
    """
    Wrap an array of host handles.
    """
    cdef list result = []
    for idx in range(count):
      result.append(Host.wrap(hosts[idx]))
    return result

  def dump(self):
    """
    Dump host info to stdout as SimGrid pleases.
    """
    self.__check_impl()
    cplatform.sg_host_dump(self.impl)

  @property
  def native(self):
    """
    Get/set host native handle as intptr_t.

    Warning:
      Be very careful with setting this property, wrong values will lead to SEGFAULT and no amout of try/excepts will save you.
    """
    return <common.intptr>self.impl

  @native.setter
  def native(self, common.intptr value):
    self.impl = <cplatform.sg_host_t>value

  @property
  def name(self):
    """
    Get host name.
    """
    self.__check_impl()
    cdef const char* cname = cplatform.sg_host_get_name(self.impl)
    return cname.decode("utf-8")

  @property
  def speed(self):
    """
    Get host speed in flops.
    """
    self.__check_impl()
    return cplatform.sg_host_speed(self.impl)

  @property
  def available_speed(self):
    """
    Get fraction of host speed avaiable (0 <= r <= 1).
    """
    self.__check_impl()
    return cplatform.sg_host_get_available_speed(self.impl)

  @property
  def data(self):
    """
    Get/set user data associated with this host.
    """
    self.__check_impl()
    return self.user_data

  @data.setter
  def data(self, object value):
    self.__check_impl()
    self.user_data = value

  @data.deleter
  def data(self):
    self.user_data = None

  def __cinit__(self):
    """
    Basic initialization.
    """
    self.impl = NULL
    self.user_data = None

  def __check_impl(self):
    """
    Validate the impl pointer.
    """
    if self.impl == NULL:
      raise RuntimeError("Host instance is NULL")
