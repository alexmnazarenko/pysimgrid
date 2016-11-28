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
cimport cplatform

cdef extern from "simgrid/simdag.h":
  ctypedef void* SD_task_t
  ctypedef void* SD_link_t

  ctypedef enum e_SD_task_state_t:
    SD_NOT_SCHEDULED = 0
    SD_SCHEDULABLE = 0x0001
    SD_SCHEDULED = 0x0002
    SD_RUNNABLE = 0x0004
    SD_RUNNING = 0x0008
    SD_DONE = 0x0010
    SD_FAILED = 0x0020

  ctypedef enum e_SD_task_kind_t:
    SD_TASK_NOT_TYPED = 0
    SD_TASK_COMM_E2E = 1
    SD_TASK_COMP_SEQ = 2
    SD_TASK_COMP_PAR_AMDAHL = 3
    SD_TASK_COMM_PAR_MXN_1D_BLOCK = 4

  # funky stuff - expose some defines as static variables
  cdef int SIMGRID_VERSION_MAJOR
  cdef int SIMGRID_VERSION_MINOR
  cdef int SIMGRID_VERSION_PATCH
  cdef char* SIMGRID_VERSION_STRING

  ##########################################################
  # Top-level API - lifetime, plaform/dag import, simulation
  ##########################################################
  void SD_init(int* argc, char** argv);
  void SD_config(char* key, char* value);
  void SD_create_environment(const char* cplatform_file);
  xbt.xbt_dynar_t SD_simulate(double how_long);
  double SD_get_clock();
  void SD_exit();
  xbt.xbt_dynar_t SD_daxload(const char* filename);
  xbt.xbt_dynar_t SD_dotload(const char* filename);
  xbt.xbt_dynar_t SD_PTG_dotload(const char* filename);
  xbt.xbt_dynar_t SD_dotload_with_sched(const char* filename);
  void uniq_transfer_task_name(SD_task_t task);


  ##########################################################
  # Task API
  ##########################################################
  SD_task_t SD_task_create(const char* name, void* data, double amount);
  void* SD_task_get_data(SD_task_t task);
  void SD_task_set_data(SD_task_t task, void* data);
  e_SD_task_state_t SD_task_get_state(SD_task_t task);
  const char* SD_task_get_name(SD_task_t task);
  void SD_task_set_name(SD_task_t task, const char* name);
  void SD_task_set_rate(SD_task_t task, double rate);

  void SD_task_watch(SD_task_t task, e_SD_task_state_t state);
  void SD_task_unwatch(SD_task_t task, e_SD_task_state_t state);
  double SD_task_get_amount(SD_task_t task);
  void SD_task_set_amount(SD_task_t task, double amount);
  double SD_task_get_alpha(SD_task_t task);
  double SD_task_get_remaining_amount(SD_task_t task);
  double SD_task_get_execution_time(SD_task_t task, int workstation_nb, const cplatform.sg_host_t* workstation_list,
                                                const double* flops_amount, const double* bytes_amount);
  e_SD_task_kind_t SD_task_get_kind(SD_task_t task);
  void SD_task_schedule(SD_task_t task, int workstation_nb, const cplatform.sg_host_t* workstation_list,
                                    const double* flops_amount, const double* bytes_amount, double rate);
  void SD_task_unschedule(SD_task_t task);
  double SD_task_get_start_time(SD_task_t task);
  double SD_task_get_finish_time(SD_task_t task);
  xbt.xbt_dynar_t SD_task_get_parents(SD_task_t task);
  xbt.xbt_dynar_t SD_task_get_children(SD_task_t task);
  int SD_task_get_workstation_count(SD_task_t task);
  cplatform.sg_host_t* SD_task_get_workstation_list(SD_task_t task);
  void SD_task_destroy(SD_task_t task);
  void SD_task_dump(SD_task_t task);
  void SD_task_dotty(SD_task_t task, void* out_FILE);

  void SD_task_schedulev(SD_task_t task, int count, const cplatform.sg_host_t* list);

  SD_task_t SD_task_create_comp_seq(const char* name, void* data, double amount);
  SD_task_t SD_task_create_comp_par_amdahl(const char* name, void* data, double amount, double alpha);
  SD_task_t SD_task_create_comm_e2e(const char* name, void* data, double amount);
  SD_task_t SD_task_create_comm_par_mxn_1d_block(const char* name, void* data, double amount);
  void SD_task_distribute_comp_amdahl(SD_task_t task, int ws_count);
  #void SD_task_schedulel(SD_task_t task, int count, ...); # disable vararg signature

cdef class Task:
  cdef SD_task_t impl
  cdef object user_data

  @staticmethod
  cdef Task wrap(SD_task_t impl)

  @staticmethod
  cdef list wrap_batch(SD_task_t* tasks, int count)
