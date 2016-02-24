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
  std::uniform_int_distribution<> dist(0, workstations.size() - 1);
  for (auto& task: _simulator->get_tasks()) {
    if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
      SD_task_schedulel(task, 1, workstations[dist(generator)]);
    }
  }
}


// ----------------------------------------------------------------------


void TrivialScheduler::register_options(po::options_description& global_options) {
  po::options_description description("Trivial scheduler options");
  description.add_options()
      ("trivial-idx", po::value<int>()->default_value(0), "select target host by index")
      ("trivial-name", po::value<std::string>(), "select target host by name. if set, has higher priority than index.")
  ;
  global_options.add(description);
}


void TrivialScheduler::_init(const po::variables_map& config) {
  Scheduler::_init(config);
  if (config.count("trivial-name")) {
    const auto target_name = config["trivial-name"].as<std::string>();
    _target_idx = -1;

    const auto workstations =_simulator->get_workstations();
    for (size_t idx = 0; idx < workstations.size(); ++idx) {
      if (SD_workstation_get_name(workstations[idx]) == target_name) {
        _target_idx = idx;
        break;
      }
    }
    if (_target_idx < 0) {
      throw std::runtime_error("workstation with a given name does not exist");
    }
  } else {
    _target_idx = config["trivial-idx"].as<int>();
    if (_target_idx < 0 || _target_idx >= static_cast<int>(_simulator->get_workstations().size())) {
      throw std::runtime_error("workstation with a given index does not exist");
    }
  }
}


void TrivialScheduler::_schedule() {
  auto workstation = _simulator->get_workstations()[_target_idx];
  for (auto& task: _simulator->get_tasks()) {
    if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ) {
      SD_task_schedulel(task, 1, workstation);
    }
  }
}


}
