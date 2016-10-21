import os
from setuptools import setup, Extension
from Cython.Build import cythonize


def local_path(*components):
  project_root = os.path.dirname(os.path.realpath(__file__))
  return os.path.normpath(os.path.join(project_root, *components))

SIMGRID_ROOT = local_path("opt/SimGrid")

EXT_OPTIONS = {
  "libraries": ["simgrid"],
  "include_dirs": [os.path.join(SIMGRID_ROOT, "include")],
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

setup(name="pysimgrid",
version="1.0.0",
author="Alexey Nazarenko",
packages=["pysimgrid", "pysimgrid.simdag"],
ext_modules=cythonize([sdwrapper, plwrapper]))
