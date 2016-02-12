// (c) DATADVANCE 2016

#include "simple_schedules.hpp"
#include "simulator.hpp"

#include <random>

namespace po = boost::program_options;

namespace darunner {

void RoundRobinScheduler::_schedule() {
  unsigned ws_idx = 0;
  auto workstations = _simulator->get_workstations();
  for (auto& task: _simulator->get_tasks()) {
    if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
      SD_task_schedulel(task, 1, workstations[ws_idx++ % workstations.size()]);
    }
  }
}


void RandomScheduler::_schedule() {
  std::mt19937 generator;
  generator.seed(std::random_device{}());
  auto workstations = _simulator->get_workstations();
  for (auto& task: _simulator->get_tasks()) {
    if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
      SD_task_schedulel(task, 1, workstations[generator() % workstations.size()]);
    }
  }
}


boost::program_options::options_description RandomScheduler::get_options() {
  boost::program_options::options_description description("Random schedule options");
  description.add_options()
      ("seed", po::value<int>()->default_value(0), "random seed to use, 0 means random initialization")
  ;
  return description;
}


}
