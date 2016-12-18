import os
from setuptools import setup, Extension

import numpy
from Cython.Build import cythonize


def local_path(*components):
  project_root = os.path.dirname(os.path.realpath(__file__))
  return os.path.normpath(os.path.join(project_root, *components))

SIMGRID_ROOT = local_path("opt/SimGrid")

EXT_OPTIONS = {
  "libraries": ["simgrid"],
  "include_dirs": [os.path.join(SIMGRID_ROOT, "include"), numpy.get_include()],
  "library_dirs": [os.path.join(SIMGRID_ROOT, "lib")],
}
if os.name != "nt":
  EXT_OPTIONS["runtime_library_dirs"] = [os.path.join(SIMGRID_ROOT, "lib")]

sdwrapper = Extension("pysimgrid.csimdag",
                      sources=["pysimgrid/csimdag.pyx"],
                      **EXT_OPTIONS)

plwrapper = Extension("pysimgrid.cplatform",
                      sources=["pysimgrid/cplatform.pyx"],
                      **EXT_OPTIONS)

scwrapper = Extension("pysimgrid.cscheduling",
                      sources=["pysimgrid/cscheduling.pyx"],
                      **EXT_OPTIONS)

setup(name="pysimgrid",
      version="1.0.0",
      author="Alexey Nazarenko",
      packages=["pysimgrid", "pysimgrid.simdag"],
      ext_modules=cythonize([sdwrapper, plwrapper, scwrapper]))
