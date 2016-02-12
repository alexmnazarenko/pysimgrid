// (c) DATADVANCE 2016

#include "scheduler.hpp"
#include "simulator.hpp"
#include <boost/assert.hpp>
#include <random>
#include <algorithm>

XBT_LOG_NEW_DEFAULT_CATEGORY(scheduler, "darunner scheduler");

namespace darunner {

namespace scheduler {

SD_workstation_t get_submission_node(Simulator& simulator) {
  SD_workstation_t result = nullptr;
  for (auto& ws: simulator.get_workstations()) {
    Simulator::WorkstationData* const data = reinterpret_cast<Simulator::WorkstationData*>(SD_workstation_get_data(ws));
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


void schedule_special_tasks(Simulator& simulator) {
  const SD_workstation_t submission_node = get_submission_node(simulator);
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

template<class T>
std::vector<T> xbt_to_vector_noown(xbt_dynar_t array) {
  if (!array) {
    throw std::runtime_error("cannot convert empty array");
  }
  std::vector<T> result;
  result.reserve(xbt_dynar_length(array));
  unsigned idx;
  T element;
  xbt_dynar_foreach(array, idx, element) {
    result.push_back(element);
  }
  xbt_dynar_free_container(&array);
  return result;
}


class RoundRobinScheduler: public Scheduler {
  virtual void schedule() {
    schedule_special_tasks(*_simulator);
    unsigned ws_idx = 0;
    auto workstations = _simulator->get_workstations();
    for (auto& task: _simulator->get_tasks()) {
      if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
        SD_task_schedulel(task, 1, workstations[ws_idx++ % workstations.size()]);
      }
    }
  }
};

class RandomScheduler: public Scheduler {
  virtual void schedule() {
    std::mt19937 generator;
    generator.seed(std::random_device{}());
    schedule_special_tasks(*_simulator);
    auto workstations = _simulator->get_workstations();
    for (auto& task: _simulator->get_tasks()) {
      if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_NOT_SCHEDULED) {
        SD_task_schedulel(task, 1, workstations[generator() % workstations.size()]);
      }
    }
  }
};

class GreedyScheduler: public Scheduler {
  int _step_no = 0;

  virtual Type type() const {
    return Type::DYNAMIC;
  }

  virtual void schedule() {
    schedule_special_tasks(*_simulator);
    auto workstations = _simulator->get_workstations();

    std::vector<SD_task_t> schedulable_tasks;

    for (auto task: _simulator->get_tasks()) {
      if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ && SD_task_get_state(task) == SD_SCHEDULABLE) {
        schedulable_tasks.push_back(task);
      }
    }

    if (schedulable_tasks.empty()) {
      return;
    }

    XBT_INFO("====================");
    XBT_INFO("Scheduling step %d", _step_no++);
    XBT_INFO("  Schedulable tasks: %lu", schedulable_tasks.size());
    for (auto task: schedulable_tasks) {
      XBT_INFO("    %s", SD_task_get_name(task));
    }

    const unsigned tasks_to_schedule = schedulable_tasks.size();
    for (unsigned schedulable_idx = 0; schedulable_idx < tasks_to_schedule; ++schedulable_idx) {

      XBT_INFO("  Optimimal resource search:");
      for (auto task: schedulable_tasks) {
        SD_workstation_t best = nullptr;
        double best_time = std::numeric_limits<double>::max();
        for (auto workstation: workstations) {
          const double completion = completion_estimate(task, workstation);
          if (completion < best_time) {
            best_time = completion;
            best = workstation;
          }
        }
        _simulator->task_get_data(task)->executor_guess = best;
        _simulator->task_get_data(task)->completion_guess = best_time;
        XBT_INFO("    %s: %s (%f)", SD_task_get_name(task), SD_workstation_get_name(best), best_time);
      }
      std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){ return _simulator->task_get_data(lhs)->completion_guess < _simulator->task_get_data(rhs)->completion_guess;});

      auto task_to_schedule = schedulable_tasks.back();
      auto task_data = _simulator->task_get_data(task_to_schedule);
      XBT_INFO("  Scheduling %s to %s (%f)", SD_task_get_name(task_to_schedule), SD_workstation_get_name(task_data->executor_guess), task_data->completion_guess);

      SD_task_schedulel(task_to_schedule, 1, task_data->executor_guess);
      _simulator->workstation_get_data(task_data->executor_guess)->available_at = task_data->completion_guess;
      schedulable_tasks.pop_back();
    }


  }


  double completion_estimate(SD_task_t task, SD_workstation_t workstation) {
    auto ws_data = _simulator->workstation_get_data(workstation);
    const double comp_time = SD_workstation_get_computation_time(workstation, SD_task_get_amount(task));

    double data_available = 0;
    for (auto parent: xbt_to_vector_noown<SD_task_t>(SD_task_get_parents(task))) {
      switch (SD_task_get_kind(parent)) {
        case SD_TASK_COMM_E2E:
          {
            auto grandparents = xbt_to_vector_noown<SD_task_t>(SD_task_get_parents(parent));
            if (grandparents.size() != 1) {
              throw std::runtime_error("wrong number of parents on end-to-end communication task");
            }
            auto grandparent = grandparents[0];
            const double communication_time = SD_route_get_communication_time(SD_task_get_workstation_list(grandparent)[0], workstation, SD_task_get_amount(parent));
            data_available = std::max(data_available, communication_time + SD_task_get_finish_time(grandparent));
          }
          break;
        case SD_TASK_COMP_SEQ:
          data_available = std::max(SD_task_get_finish_time(parent), data_available);
          break;
      }
    }

    return std::max(ws_data->available_at, data_available) + comp_time;
  }
};

}

std::unique_ptr<Scheduler> Scheduler::create(Algorithm algo, Simulator& simulator) {
  std::unique_ptr<Scheduler> result;
  switch (algo) {
  case Algorithm::ROUND_ROBIN:
    result.reset(new scheduler::RoundRobinScheduler);
    break;
  case Algorithm::RANDOM:
    result.reset(new scheduler::RandomScheduler);
    break;
  case Algorithm::GREEDY:
    result.reset(new scheduler::GreedyScheduler);
    break;
  default:
    break;
  }
  result->_simulator = &simulator;
  return result;
}

Scheduler::Type Scheduler::type() const {
  return Type::STATIC;
}

}
