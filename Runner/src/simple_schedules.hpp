// (c) DATADVANCE 2016

#pragma once

#include "scheduler.hpp"

namespace darunner {

class RoundRobinScheduler: public Scheduler {
public:
protected:
  virtual void _schedule();
};

class RandomScheduler: public Scheduler {
public:
  static boost::program_options::options_description get_options();
protected:
  virtual void _schedule();
};


}
