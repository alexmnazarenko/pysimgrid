// (c) DATADVANCE 2016

#pragma once

#include "scheduler.hpp"

#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <vector>
#include <string>
#include <memory>

namespace simulate {

/**
 * SimGrid simulator state wrapper.
 *
 * - Initializes SimGrid data structures and manages their lifetime
 * - Provides some convenience methods
 *
 * @note For now I don't even try to fully incapsulate SimGrid interface but it may be required later.
 */
class SimulatorState {
public:
  /**
   * Task description file format.
   *
   * @note Embedded SimGrid formats (dot/dax) do not give access to full capabilites of simulator (e.g. multicore tasks).
   *       However, they are convenient to use because there are a lot of ready workflows in them.
   */
  enum TaskFormat {
    AUTO, ///< Select task file format by extension
    DOT,  ///< Simgrid DOT (*.dot)
    DAX,  ///< Pegasus DAX format (*.dax, *.xml)
    JSON  ///< Custom JSON format. For future use. (*.json)
  };

  /**
   * Custom data attached to all tasks.
   */
  struct TaskData {
  };

  /**
   * Custom data attached to workstations.
   */
  struct WorkstationData {
    bool is_submission_node = false; ///< Submission node is the node which hosts 'root' and 'end' tasks (simulating initial data submission & result retrieval)
  };

  /** Aux root (source) task name */
  static const std::string ROOT_TASK;
  /** Aux end (sink) task name */
  static const std::string END_TASK;

  SimulatorState(const std::string& platform_path, const std::string& tasks_path, TaskFormat task_format = TaskFormat::AUTO);
  ~SimulatorState() noexcept;

  /** Get all workstations in current platform */
  std::vector<SD_workstation_t>& get_workstations() { return _workstations; }
  /** Get all links in current platform */
  std::vector<SD_link_t>& get_links() { return _links; }
  /** Get all loaded tasks */
  std::vector<SD_task_t>& get_tasks() { return _tasks; }

  WorkstationData& workstation_data(SD_workstation_t const workstation);
  TaskData& task_data(SD_task_t const task);

  /**
   * Get task handle by name.
   *
   * For unknown reason, SimGrid provides similar method for workstations, but not for tasks.
   */
  SD_task_t task_by_name(const std::string& name);

  /**
   * Run SimGrid simulation and report true if there any tasks that changed their state.
   */
  bool simulate();

private:
  /** Shouldn't be ever copied */
  SimulatorState(const SimulatorState&);

  void _load_tasks_dot(const std::string& tasks_path);
  void _load_tasks_dax(const std::string& tasks_path);
  void _load_tasks_json(const std::string& tasks_path);

  std::vector<SD_workstation_t> _workstations;
  std::vector<SD_link_t> _links;
  std::vector<SD_task_t> _tasks;
};

}
