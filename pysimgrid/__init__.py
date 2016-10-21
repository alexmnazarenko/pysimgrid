# This file is part of pysimgrid, a Python interface to the SimGrid library.
#
# Copyright 2015-2016 Alexey Nazarenko and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from . import csimdag
from . import cplatform
from . import simdag

from . import _version
__version__ = _version.version
__simgrid_version__ = _version.simgrid_version
