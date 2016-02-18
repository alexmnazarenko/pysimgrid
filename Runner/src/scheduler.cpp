// A. Nazarenko 2016

#include "scheduler.hpp"
#include "simple_schedules.hpp"
#include "list_heuristics.hpp"
#include "simulator.hpp"
#include "mp_utils.hpp"

#include <boost/assert.hpp>
#include <algorithm>

XBT_LOG_NEW_DEFAULT_CATEGORY(scheduler, "darunner scheduler");

namespace po = boost::program_options;

namespace simulate {

/**
 * Schedulers registry
 */
typedef mp::Typelist<RoundRobinScheduler, RandomScheduler, ListHeuristic> SchedulerTypes;

namespace mp {
struct GetNames {
  std::vector<std::string> result;

  template<class T>
  bool visit() {
    result.push_back(T::name());
    return true;
  }
};


template<class Base>
struct Create {
  std::unique_ptr<Base> result;
  std::string requested_name;

  Create(const std::string& requested): requested_name(requested) {}

  template<class T>
  bool visit() {
    if (T::name() == requested_name) {
      result.reset(new T);
      return false;
    }
    return true;
  }
};


struct RegisterOptions {
  po::options_description& global_description;

  RegisterOptions(po::options_description& global_description_): global_description(global_description_) {}

  template<class T>
  bool visit() {
    T::register_options(global_description);
    return true;
  }
};
}

std::unique_ptr<Scheduler> Scheduler::create(const std::string& algoritm_name) {
  std::unique_ptr<Scheduler> result(mp::apply_visitor<mp::Create<Scheduler>, SchedulerTypes>(algoritm_name).result);
  if (!result) {
    throw std::runtime_error("unknown scheduler algorithm requested");
  }
  return result;
}


std::vector<std::string> Scheduler::names() {
  return mp::apply_visitor<mp::GetNames, SchedulerTypes>().result;
}


void Scheduler::register_options(po::options_description& options) {
  // Add common options
  po::options_description description("Common options");
  description.add_options()
      ("seed", po::value<int>()->default_value(0), "random seed to use in randomized schedules, 0 means random initialization")
  ;
  options.add(description);
  // Add per-algorithm options
  mp::apply_visitor<mp::RegisterOptions, SchedulerTypes>(options);
}


void Scheduler::run(SimulatorState& simulator, const boost::program_options::variables_map& config) {
  _simulator = &simulator;
  _step_no = 0;

  _init(config);

  const double start = SD_get_clock();
  switch (_type()) {
  case Type::STATIC:
    _schedule();
    _simulator->simulate();
    break;
  case Type::REACTIVE:
    for (auto task: _simulator->get_tasks()) {
      if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ) {
        SD_task_watch(task, SD_DONE);
      }
    }
    _schedule();
    ++_step_no;
    while (_simulator->simulate()) {
      _schedule();
      ++_step_no;
    }
    break;
  default:
    throw std::runtime_error("unknown scheduler type");
  }
  XBT_INFO("Execution time: %f seconds\n", SD_get_clock() - start);
}


void Scheduler::_init(const boost::program_options::variables_map&) {
}


Scheduler::Type Scheduler::_type() const {
  return Type::STATIC;
}


SD_workstation_t Scheduler::_get_submission_node(SimulatorState& simulator) {
  SD_workstation_t result = nullptr;
  for (auto& ws: simulator.get_workstations()) {
    const auto data = simulator.workstation_data(ws);
    if (data.is_submission_node) {
      result = ws;
      break;
    }
  }
  if (!result) {
    result = simulator.get_workstations()[0];
  }
  return result;
}


void Scheduler::_schedule_special_tasks(SimulatorState& simulator) {
  const SD_workstation_t submission_node = _get_submission_node(simulator);
  BOOST_ASSERT(submission_node);
  const SD_task_t root = simulator.task_by_name(SimulatorState::ROOT_TASK);
  const SD_task_t end = simulator.task_by_name(SimulatorState::END_TASK);
  if (SD_task_get_state(root) != SD_SCHEDULED && SD_task_get_state(root) != SD_DONE) {
    SD_task_schedulel(root, 1, submission_node);
  }
  if (SD_task_get_state(end) != SD_SCHEDULED && SD_task_get_state(end) != SD_DONE) {
    SD_task_schedulel(end, 1, submission_node);
  }
}

}
