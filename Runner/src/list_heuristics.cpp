// (c) DATADVANCE 2016

#include "list_heuristics.hpp"
#include "simulator.hpp"

#include <random>

XBT_LOG_NEW_DEFAULT_CATEGORY(list_heuristics, "darunner list heuristics");

namespace po = boost::program_options;

namespace darunner {

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
      ("lh_strategy", po::value<std::string>()->default_value("min"), "strategy to priotirize tasks in list - when scheduling multiple parallel tasks, which of them should be its best resource. "
                                                                      "valid values are: min, max, sufferage")
  ;
  global_options.add(description);
}


ListHeuristic::Type ListHeuristic::type() const {
  return Type::DYNAMIC;
}


void ListHeuristic::_init(const boost::program_options::variables_map& config) {
  Scheduler::_init(config);

  const std::string strategy = config["lh_strategy"].as<std::string>();
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
        //XBT_INFO("    %s: %s (%f)", SD_task_get_name(task), SD_workstation_get_name(workstation), completion);
      }
      std::sort(_task_states[task].estimates.begin(), _task_states[task].estimates.end(),
      [](const std::pair<SD_workstation_t, double>& lhs, const std::pair<SD_workstation_t, double>& rhs) {
        return lhs.second < rhs.second;
      });
      const auto& best = _task_states[task].estimates[0];
      XBT_INFO("    best for %s: %s (%f)", SD_task_get_name(task), SD_workstation_get_name(best.first), best.second);
    }
    switch (_strategy) {
    case Strategy::MIN_FIRST:
      std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){ return _task_states[lhs].estimates[0].second > _task_states[rhs].estimates[0].second;});
      break;
    case Strategy::MAX_FIRST:
      std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){ return _task_states[lhs].estimates[0].second < _task_states[rhs].estimates[0].second;});
      break;
    case Strategy::SUFFERAGE:
      throw std::runtime_error("strategy 'sufferage' is not supported yet");
      //std::sort(schedulable_tasks.begin(), schedulable_tasks.end(), [this](const SD_task_t& lhs, const SD_task_t& rhs){ return _task_states[lhs].completion_guess > _task_states[rhs].completion_guess;});
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
  XBT_INFO("    %s at %s: %f %f %f", SD_task_get_name(task), SD_workstation_get_name(workstation), std::max(ws_data.available_at, SD_get_clock()), data_available, comp_time);
  return std::max(data_available, std::max(ws_data.available_at, SD_get_clock())) + comp_time;
}

}
