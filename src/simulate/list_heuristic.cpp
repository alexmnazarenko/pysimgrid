// A. Nazarenko 2016

#include "list_heuristic.hpp"
#include "simulator.hpp"

#include <random>


XBT_LOG_EXTERNAL_CATEGORY(simulate);
XBT_LOG_NEW_DEFAULT_SUBCATEGORY(list_heuristics, simulate, "list heuristics");

namespace po = boost::program_options;

namespace simulate {

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


void ListHeuristic::register_options(po::options_description& global_options) {
  po::options_description description("List heuristic options");
  description.add_options()
      ("lh-strategy", po::value<std::string>()->default_value("min"), "strategy to priotirize tasks in batch. highest priority task gets its best resource. "
                                                                      "valid values are: min, max, sufferage.")
  ;
  global_options.add(description);
}


ListHeuristic::Type ListHeuristic::_type() const {
  return Type::DYNAMIC;
}


void ListHeuristic::_init(const boost::program_options::variables_map& config) {
  Scheduler::_init(config);

  const std::string strategy = config["lh-strategy"].as<std::string>();
  if (strategy == "min") {
    _strategy = Strategy::MIN_FIRST;
  } else if (strategy == "max") {
    _strategy = Strategy::MAX_FIRST;
  } else if (strategy == "sufferage") {
    _strategy = Strategy::SUFFERAGE;
  } else {
    throw std::runtime_error("wrong lh_strategy option value!");
  }
  XBT_INFO("Using priority strategy: %s", strategy.c_str());
}


void ListHeuristic::_schedule() {
  if (_step_no == 0) {
    _schedule_special_tasks(*_simulator);
  }

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
  XBT_INFO("Scheduling step %d", _step_no);
  XBT_INFO("  Schedulable tasks: %lu", schedulable_tasks.size());
  for (auto task: schedulable_tasks) {
    XBT_INFO("    %s", SD_task_get_name(task));
  }

  const unsigned tasks_to_schedule = schedulable_tasks.size();
  for (unsigned schedulable_idx = 0; schedulable_idx < tasks_to_schedule; ++schedulable_idx) {
    XBT_INFO("  Optimimal resource search:");
    for (auto task: schedulable_tasks) {
      _task_states[task].estimates.clear();
      _task_states[task].estimates.reserve(workstations.size());
      for (auto workstation: workstations) {
        const double completion = _completion_estimate(task, workstation);
        _task_states[task].estimates.push_back(std::make_pair(workstation, completion));
      }
      std::sort(_task_states[task].estimates.begin(), _task_states[task].estimates.end(),
      [](const std::pair<SD_workstation_t, double>& lhs, const std::pair<SD_workstation_t, double>& rhs) {
        return lhs.second < rhs.second;
      });
      const auto& best = _task_states[task].estimates[0];
      XBT_INFO("    best for %s: %s (%f)", SD_task_get_name(task), SD_workstation_get_name(best.first), best.second);
    }

    // Sort in such a way that a priority task will be on the *back* of the vector (so it is easier to pop).
    //
    // Priority is defined according to a selected strategy (highest priority tasks are scheduled first, so they can get their 'preffered' resource):
    //   1) min - prioritize 'fastest' (on a best available resource) tasks
    //   2) max - prioritize 'slowest' (on a best available resource) tasks first
    //   3) sufferage - prioritize tasks that 'suffer' most if the second best resource is selected
    //
    switch (_strategy) {
    case Strategy::MIN_FIRST:
      std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){
        return _task_states[lhs].estimates[0].second > _task_states[rhs].estimates[0].second;
      });
      break;
    case Strategy::MAX_FIRST:
      std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){
        return _task_states[lhs].estimates[0].second < _task_states[rhs].estimates[0].second;
      });
      break;
    case Strategy::SUFFERAGE:
      std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){
        const auto& lhs_estimates = _task_states[lhs].estimates;
        const auto& rhs_estimates = _task_states[rhs].estimates;
        double lhs_sufferage = lhs_estimates[0].second;
        double rhs_sufferage = rhs_estimates[0].second;
        BOOST_ASSERT(lhs_estimates.size() == rhs_estimates.size());
        if (lhs_estimates.size() > 1) {
          lhs_sufferage = lhs_estimates[1].second - lhs_estimates[0].second;
          rhs_sufferage = rhs_estimates[1].second - rhs_estimates[0].second;
        }
        return lhs_sufferage < rhs_sufferage;
      });
      break;
    default:
      throw std::runtime_error("unknown strategy requested");
    }

    auto task_to_schedule = schedulable_tasks.back();
    auto task_data = _task_states[task_to_schedule];
    auto target_ws = task_data.estimates[0].first;
    auto completion = task_data.estimates[0].second;
    XBT_INFO("  Scheduling %s to %s (%f)", SD_task_get_name(task_to_schedule), SD_workstation_get_name(target_ws), completion);

    SD_task_schedulel(task_to_schedule, 1, target_ws);
    _workstation_states[target_ws].available_at = completion;
    schedulable_tasks.pop_back();
  }
}


double ListHeuristic::_completion_estimate(SD_task_t task, SD_workstation_t workstation) {
  auto ws_data = _workstation_states[workstation];
  const double comp_time = SD_workstation_get_computation_time(workstation, SD_task_get_amount(task));

  double communication_time = 0;
  for (auto parent: xbt_to_vector_noown<SD_task_t>(SD_task_get_parents(task))) {
    switch (SD_task_get_kind(parent)) {
      case SD_TASK_COMM_E2E:
        {
          auto grandparents = xbt_to_vector_noown<SD_task_t>(SD_task_get_parents(parent));
          if (grandparents.size() != 1) {
            throw std::runtime_error("wrong number of parents on end-to-end communication task");
          }
          auto grandparent = grandparents[0];
          const double communication = SD_route_get_communication_time(SD_task_get_workstation_list(grandparent)[0], workstation, SD_task_get_amount(parent));
          communication_time = std::max(communication_time, communication);
        }
        break;
      case SD_TASK_COMP_SEQ:
        break;
    }
  }

  return std::max(ws_data.available_at, SD_get_clock()) + communication_time + comp_time;
}

}
