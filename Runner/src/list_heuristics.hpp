// (c) DATADVANCE 2016

#pragma once

#include "scheduler.hpp"

namespace darunner {

class ListHeuristic: public Scheduler {
public:
  static std::string name() { return "list_heuristic"; }
  static void register_options(boost::program_options::options_description& global_options);

  virtual Type type() const;
protected:
  struct WorkstationState {
    double available_at = 0.;
  };

  struct TaskState {
    SD_workstation_t executor_guess = nullptr;
    double completion_guess = 0;
  };

  virtual void _init(const boost::program_options::variables_map& config);
  virtual void _schedule();
  double _completion_estimate(SD_task_t task, SD_workstation_t workstation);

  std::map<SD_workstation_t, WorkstationState> _workstation_states;
  std::map<SD_task_t, TaskState> _task_states;
};


}
