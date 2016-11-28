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
Very basic wrapper over SimGrid/XBT library types, only the absolutely required functions.

Intent is to avoid using them as much as possible. However, SimGrid API sometimes just returns them, so we need at least RO access.
"""

cdef extern from "xbt/dynar.h":
  ctypedef void* xbt_dynar_t

  unsigned long xbt_dynar_length(const xbt_dynar_t dynar);
  int xbt_dynar_is_empty(const xbt_dynar_t dynar);
  void xbt_dynar_get_cpy(const xbt_dynar_t dynar, const unsigned long idx, void* const dst);

  void* xbt_dynar_to_array(xbt_dynar_t dynar);

  void xbt_dynar_free(xbt_dynar_t* dynar);
  void xbt_dynar_free_container(xbt_dynar_t* dynar);


cdef extern from "xbt/dict.h":
  ctypedef void* xbt_dict_t


cdef extern from "xbt/log.h":
  void xbt_log_control_set(const char* cs);
