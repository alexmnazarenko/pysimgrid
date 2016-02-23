// A. Nazarenko 2016

#include "simple_schedules.hpp"
#include "simulator.hpp"

#include <random>

namespace po = boost::program_options;

namespace simulate {

void RoundRobinScheduler::_schedule() {
  _schedule_special_tasks(*_simulator);
  unsigned ws_idx = 0;
  auto workstations = _simulator->get_workstations();
  for (auto& task: _simulator->get_tasks()) {
    if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
      SD_task_schedulel(task, 1, workstations[ws_idx++ % workstations.size()]);
    }
  }
}

// ----------------------------------------------------------------------


void RandomScheduler::_init(const po::variables_map& config) {
  Scheduler::_init(config);
  _seed = config["seed"].as<int>();
}


void RandomScheduler::_schedule() {
  _schedule_special_tasks(*_simulator);
  std::mt19937 generator;
  generator.seed(_seed ? _seed : std::random_device{}());
  auto workstations = _simulator->get_workstations();
  for (auto& task: _simulator->get_tasks()) {
    if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
      SD_task_schedulel(task, 1, workstations[generator() % workstations.size()]);
    }
  }
}


}
