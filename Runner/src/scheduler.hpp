// (c) DATADVANCE 2016

#pragma once

#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <boost/program_options.hpp>
#include <memory>
#include <map>

namespace darunner {

class Simulator;

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

  static std::unique_ptr<Scheduler> create(Algorithm algo);
  static void register_options(boost::program_options::options_description& options);

  virtual void init(Simulator& simulator);
  virtual Type type() const;
  virtual void schedule();

protected:
  virtual void _schedule() = 0;

  static SD_workstation_t _get_submission_node(Simulator& simulator);
  static void _schedule_special_tasks(Simulator& simulator);

  unsigned _step_no = 0;
  Simulator* _simulator = nullptr;
};

}
