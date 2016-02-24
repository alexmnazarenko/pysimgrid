// A. Nazarenko 2016

#pragma once

#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <boost/program_options.hpp>
#include <memory>
#include <map>

namespace simulate {

class SimulatorState;

/**
 * Scheduler base class.
 *
 * - Defines scheduler virtual interface
 * - Enables easy instantiation & configuration of derived classes
 *
 * @note to register a new scheduler you need to add to SchedulerTypes list.
 */
class Scheduler {
public:
  enum class Type {
    STATIC,  ///< Schedules all tasks at once
    DYNAMIC  ///< Schedules tasks as they become available
  };

  /**
   * Create scheduler by algorithm name.
   */
  static std::unique_ptr<Scheduler> create(const std::string& algoritm_name);
  /**
   * Get all known algorithm names.
   */
  static std::vector<std::string> names();
  /**
   * Register options for all algorithms.
   */
  static void register_options(boost::program_options::options_description& options);

  void run(SimulatorState& simulator, const boost::program_options::variables_map& config);

protected:
  Scheduler();

  /**
   * Read configuration & inintialize aux data before scheduling.
   */
  virtual void _init(const boost::program_options::variables_map&);
  /**
   * Get scheduler type.
   */
  virtual Type _type() const;
  /**
   * Perform scheduling step (should schedule all tasks if type is STATIC).
   */
  virtual void _schedule() = 0;

  static SD_workstation_t _get_submission_node(SimulatorState& simulator);
  static void _schedule_special_tasks(SimulatorState& simulator);

  void _dump_state(double start_time, const std::string& path);

  unsigned _step_no = 0;
  SimulatorState* _simulator = nullptr;
};

}
