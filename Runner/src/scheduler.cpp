// (c) DATADVANCE 2016

#include "scheduler.hpp"
#include "simple_schedules.hpp"
#include "list_heuristics.hpp"
#include "simulator.hpp"

#include <boost/assert.hpp>
#include <algorithm>

XBT_LOG_NEW_DEFAULT_CATEGORY(scheduler, "darunner scheduler");

namespace darunner {

std::unique_ptr<Scheduler> Scheduler::create(Algorithm algo) {
  std::unique_ptr<Scheduler> result;
  switch (algo) {
  case Algorithm::ROUND_ROBIN:
    result.reset(new RoundRobinScheduler);
    break;
  case Algorithm::RANDOM:
    result.reset(new RandomScheduler);
    break;
  case Algorithm::GREEDY:
    result.reset(new GreedyScheduler);
    break;
  default:
    break;
  }
  return result;
}


void Scheduler::register_options(boost::program_options::options_description& options) {
  options.add(RandomScheduler::get_options());
}


void Scheduler::init(Simulator& simulator) {
  _simulator = &simulator;
  _schedule_special_tasks(simulator);
}


Scheduler::Type Scheduler::type() const {
  return Type::STATIC;
}


void Scheduler::schedule() {
  if (!_simulator) {
    throw std::runtime_error("init must be called first");
  }
  if (type() == Type::STATIC && _step_no > 0) {
    throw std::runtime_error("multiple calls to static scheduler");
  }
  _schedule();
  ++_step_no;
}


SD_workstation_t Scheduler::_get_submission_node(Simulator& simulator) {
  SD_workstation_t result = nullptr;
  for (auto& ws: simulator.get_workstations()) {
    Simulator::WorkstationData* const data = simulator.workstation_get_data(ws);
    if (data->is_submission_node) {
      result = ws;
      break;
    }
  }
  if (!result) {
    result = simulator.get_workstations()[0];
  }
  return result;
}


void Scheduler::_schedule_special_tasks(Simulator& simulator) {
  const SD_workstation_t submission_node = _get_submission_node(simulator);
  BOOST_ASSERT(submission_node);
  SD_task_t root = simulator.task_by_name(Simulator::ROOT_TASK);
  SD_task_t end = simulator.task_by_name(Simulator::END_TASK);
  if (SD_task_get_state(root) != SD_SCHEDULED && SD_task_get_state(root) != SD_DONE) {
    SD_task_schedulel(root, 1, submission_node);
  }
  if (SD_task_get_state(end) != SD_SCHEDULED && SD_task_get_state(end) != SD_DONE) {
    SD_task_schedulel(end, 1, submission_node);
  }
}

}
