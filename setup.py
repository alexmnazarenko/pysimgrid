import os
from setuptools import setup, Extension

import numpy

try:
  from Cython.Build import cythonize
  USE_CYTHON = True
except ImportError:
  USE_CYTHON = False


def local_path(*components):
  project_root = os.path.dirname(os.path.realpath(__file__))
  return os.path.normpath(os.path.join(project_root, *components))

def source_file(filename):
  return os.path.join("pysimgrid", filename) + (".pyx" if USE_CYTHON else ".c")

SIMGRID_ROOT = local_path("opt/SimGrid")

EXT_OPTIONS = {
  "libraries": ["simgrid"],
  "include_dirs": [os.path.join(SIMGRID_ROOT, "include"), numpy.get_include()],
  "library_dirs": [os.path.join(SIMGRID_ROOT, "lib")],
}
if os.name != "nt":
  EXT_OPTIONS["runtime_library_dirs"] = [os.path.join(SIMGRID_ROOT, "lib")]

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
if USE_CYTHON:
  extensions = cythonize(extensions, compiler_directives={'embedsignature': True})

setup(name="pysimgrid",
      version="1.0.0",
      author="Alexey Nazarenko",
      packages=["pysimgrid", "pysimgrid.simdag"],
      ext_modules=extensions)
