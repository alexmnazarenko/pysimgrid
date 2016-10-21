# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from . import csimdag

version = "1.0.0"
simgrid_version = ".".join(map(str, [csimdag.LIBRARY_VERSION_MAJOR, csimdag.LIBRARY_VERSION_MINOR, csimdag.LIBRARY_VERSION_PATCH]))
