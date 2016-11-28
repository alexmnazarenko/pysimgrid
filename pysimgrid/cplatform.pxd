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

cimport xbt

cdef extern from "simgrid/simdag.h":
  ctypedef void* SD_link_t
  ctypedef void* sg_host_t

  ##########################################################
  # Platform API
  ##########################################################
  size_t sg_host_count();
  sg_host_t* sg_host_list();

  sg_host_t sg_host_by_name(const char* name);
  sg_host_t sg_host_by_name_or_create(const char* name);
  const char* sg_host_get_name(sg_host_t host);

  double sg_host_speed(sg_host_t host);
  double sg_host_get_available_speed(sg_host_t host);

  xbt.xbt_dict_t sg_host_get_properties(sg_host_t host);
  const char* sg_host_get_property_value(sg_host_t host, const char* name);
  void sg_host_dump(sg_host_t ws);

  void* sg_host_user(sg_host_t host);
  void sg_host_user_set(sg_host_t host, void* userdata);
  void sg_host_user_destroy(sg_host_t host);

  SD_link_t* SD_route_get_list(sg_host_t src, sg_host_t dst);
  int SD_route_get_size(sg_host_t src, sg_host_t dst);
  const char* sg_link_name(SD_link_t link);

  double SD_route_get_latency(sg_host_t src, sg_host_t dst);
  double SD_route_get_bandwidth(sg_host_t src, sg_host_t dst);

  # undocumneted advanced functions
  #int sg_host_get_nb_pstates(sg_host_t host);
  #int sg_host_get_pstate(sg_host_t host);
  #void sg_host_set_pstate(sg_host_t host, int pstate);
  #size_t sg_host_extension_create(void(*deleter)(void*));
  #void* sg_host_extension_get(sg_host_t host, size_t rank);
  #xbt.xbt_dynar_t sg_hosts_as_dynar();

cdef class Host:
  cdef sg_host_t impl
  cdef object user_data

  @staticmethod
  cdef Host wrap(sg_host_t impl)

  @staticmethod
  cdef list wrap_batch(sg_host_t* hosts, int count)
