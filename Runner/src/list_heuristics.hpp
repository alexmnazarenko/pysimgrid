// (c) DATADVANCE 2016

#pragma once

#include "scheduler.hpp"

namespace darunner {

class GreedyScheduler: public Scheduler {
public:
  virtual Type type() const;
protected:
  struct WorkstationState {
    double available_at = 0.;
  };

  struct TaskState {
    SD_workstation_t executor_guess = nullptr;
    double completion_guess = 0;
  };

  virtual void _schedule();
  double _completion_estimate(SD_task_t task, SD_workstation_t workstation);

  std::map<SD_workstation_t, WorkstationState> _workstation_states;
  std::map<SD_task_t, TaskState> _task_states;
};


}
