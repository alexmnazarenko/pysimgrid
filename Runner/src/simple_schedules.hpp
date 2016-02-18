// A. Nazarenko 2016

#pragma once

#include "scheduler.hpp"

namespace simulate {

class RoundRobinScheduler: public Scheduler {
public:
  static std::string name() { return "round_robin"; }
  static void register_options(boost::program_options::options_description&) {}
protected:
  virtual void _schedule();
};

class RandomScheduler: public Scheduler {
public:
  static std::string name() { return "random"; }
  static void register_options(boost::program_options::options_description&) {}
protected:
  virtual void _init(const boost::program_options::variables_map& vm);
  virtual void _schedule();
private:
  int _seed;
};


}
