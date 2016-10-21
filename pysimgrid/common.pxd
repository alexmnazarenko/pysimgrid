# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

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
