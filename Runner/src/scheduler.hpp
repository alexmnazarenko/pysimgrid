// (c) DATADVANCE 2016

#pragma once

#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <boost/program_options.hpp>
#include <memory>
#include <map>

namespace darunner {

class SimulatorState;

class Scheduler {
public:
  enum class Algorithm {
    ROUND_ROBIN,
    RANDOM,
    GREEDY
  };

  enum class Type {
    STATIC,
    DYNAMIC
  };

  static std::unique_ptr<Scheduler> create(const std::string& algoritm_name);
  static std::vector<std::string> names();
  static void register_options(boost::program_options::options_description& options);

  virtual Type type() const;
  void run(SimulatorState& simulator, const boost::program_options::variables_map& config);

protected:
  virtual void _init(const boost::program_options::variables_map&);
  virtual void _schedule() = 0;

  static SD_workstation_t _get_submission_node(SimulatorState& simulator);
  static void _schedule_special_tasks(SimulatorState& simulator);

  unsigned _step_no = 0;
  SimulatorState* _simulator = nullptr;
};

}
