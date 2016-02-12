// (c) DATADVANCE 2016

#pragma once

#include <memory>

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

  static const std::string ROOT_TASK;
  static const std::string END_TASK;

  static std::unique_ptr<Scheduler> create(Algorithm algo, Simulator& simulator);

  virtual Type type() const;
  virtual void schedule() = 0;

protected:
  Simulator* _simulator;
};

}
