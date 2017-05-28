from __future__ import print_function

import os
import sys
import platform
import textwrap
from setuptools import setup, Extension

import numpy

from Cython.Build import cythonize


def local_path(*components):
  project_root = os.path.dirname(os.path.realpath(__file__))
  return os.path.normpath(os.path.join(project_root, *components))


def source_file(filename):
  return os.path.join("pysimgrid", filename) + ".pyx"


simgrid_env_path = os.environ.get("SIMGRID_ROOT")
SIMGRID_ROOT = simgrid_env_path if simgrid_env_path else local_path("opt/SimGrid")

# Note that SimGrid shared library is required at runtime, so you shouldn't
# move/delete used SimGrid installation while using pysimgrid.
if not os.path.isdir(SIMGRID_ROOT):
  print(textwrap.dedent("""\
  SimGrid root directory is not found.

  SimGrid can be built manually or using the provided get_simgrid.sh script.
  pysimgrid isn't intended to be used with system-wide installations of SimGrid.

  In case of a custom build, you can select your
  install location using SIMGRID_ROOT environment variable.
  """))
  sys.exit(1)

EXT_OPTIONS = {
  "libraries": ["simgrid"],
  "include_dirs": [os.path.join(SIMGRID_ROOT, "include"), numpy.get_include()],
  "library_dirs": [os.path.join(SIMGRID_ROOT, "lib")],
}
if platform.system().lower() == "linux":
  EXT_OPTIONS["runtime_library_dirs"] = [os.path.join(SIMGRID_ROOT, "lib")]
elif platform.system().lower() == "darwin":
  EXT_OPTIONS["extra_link_args"] = ["-Wl,-rpath," + os.path.join(SIMGRID_ROOT, "lib")]

sdwrapper = Extension("pysimgrid.csimdag",
                      sources=[source_file("csimdag")],
                      **EXT_OPTIONS)

plwrapper = Extension("pysimgrid.cplatform",
                      sources=[source_file("cplatform")],
                      **EXT_OPTIONS)

scwrapper = Extension("pysimgrid.cscheduling",
                      sources=[source_file("cscheduling")],
                      **EXT_OPTIONS)

extensions = [sdwrapper, plwrapper, scwrapper]
extensions = cythonize(extensions, compiler_directives={'embedsignature': True})

setup(name="pysimgrid",
      version="1.0.0",
      author="Alexey Nazarenko",
      packages=["pysimgrid", "pysimgrid.simdag"],
      ext_modules=extensions)
