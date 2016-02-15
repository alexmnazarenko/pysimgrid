// (c) DATADVANCE 2016

#pragma once

#include "scheduler.hpp"

#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <vector>
#include <string>
#include <memory>

namespace darunner {

class SimulatorState {
public:
  enum TaskFormat {
    TASK_FORMAT_DOT,
    TASK_FORMAT_JSON
  };

  struct TaskData {
  };

  struct WorkstationData {
    bool is_submission_node = false;
  };

  static const std::string ROOT_TASK;
  static const std::string END_TASK;

  SimulatorState(const std::string& platform_path, const std::string& tasks_path, TaskFormat task_format = TASK_FORMAT_DOT);
  ~SimulatorState() noexcept;

  std::vector<SD_workstation_t>& get_workstations() { return _workstations; }
  std::vector<SD_link_t>& get_links() { return _links; }
  std::vector<SD_task_t>& get_tasks() { return _tasks; }

  WorkstationData* workstation_get_data(SD_workstation_t const workstation);
  TaskData* task_get_data(SD_task_t const task);

  SD_task_t task_by_name(const std::string& name);

  /**
   * Run SimGrid simulation and report true if there any tasks that changed their state.
   */
  bool simulate();

private:
  // Shouldn't be ever copied
  SimulatorState(const SimulatorState&);

  void _load_tasks_dot(const std::string& tasks_path);
  void _load_tasks_json(const std::string& tasks_path);

  std::vector<SD_workstation_t> _workstations;
  std::vector<SD_link_t> _links;
  std::vector<SD_task_t> _tasks;
};

}
