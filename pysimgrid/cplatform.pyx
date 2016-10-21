# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

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
  cdef int links_count = cplatform.SD_route_get_size(src.impl, dst.impl)
  if not links_count:
    return []
  cdef cplatform.SD_link_t* links = cplatform.SD_route_get_list(src.impl, dst.impl)
  cdef list result = []
  for idx in range(links_count):
    result.append(cplatform.sg_link_name(links[idx]).decode("utf-8"))
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

    Warning: we very careful with setting this property,
             wrong values will lead to SEGFAULT and no amout of try/excepts will save you
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
