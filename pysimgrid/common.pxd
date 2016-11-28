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
General utility definitions.
"""

# intptr type to expose native pointers to python
from libc.stdint cimport uintptr_t as intptr
# dynamic memory management
from libc.stdlib cimport malloc, free

# Import Python reference counting API for some darkmagic (storing pyobjects in C)
cdef extern from "Python.h":
  void Py_INCREF(object o)
  void Py_DECREF(object o)

cdef inline unicode unicode_string(s):
  """
  Unicode strings adapter adapted from Cython docs.

  Recieves an 'object' argument and ensures it to be a unicode string.
  """
  if type(s) is unicode:
    return <unicode>s
  elif isinstance(s, bytes):
    return (<bytes>s).decode("utf-8")
  elif isinstance(s, unicode):
    return unicode(s)
  else:
    raise TypeError("wrong argument type, unicode string is expected")

cdef inline bytes utf8_string(s):
  """
  Ensure a UTF-8 byte string.
  """
  return unicode_string(s).encode("utf-8")
