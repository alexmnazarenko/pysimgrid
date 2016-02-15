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
  enum class Strategy {
    MIN_FIRST,
    MAX_FIRST,
    SUFFERAGE,
    UKNOWN
  };

  struct WorkstationState {
    double available_at = 0.;
  };

  struct TaskState {
    std::vector<std::pair<SD_workstation_t, double>> estimates;
  };

  virtual void _init(const boost::program_options::variables_map& config);
  virtual void _schedule();
  double _completion_estimate(SD_task_t task, SD_workstation_t workstation);

  std::map<SD_workstation_t, WorkstationState> _workstation_states;
  std::map<SD_task_t, TaskState> _task_states;
  Strategy _strategy = Strategy::UKNOWN;
};


}
