// A. Nazarenko 2016

#include "scheduler.hpp"
#include "simple_schedules.hpp"
#include "list_heuristic.hpp"
#include "simulator.hpp"
#include "mp_utils.hpp"

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/filewritestream.h>
#include <rapidjson/prettywriter.h>
#include <boost/assert.hpp>
#include <algorithm>

XBT_LOG_EXTERNAL_CATEGORY(simulate);
XBT_LOG_NEW_DEFAULT_SUBCATEGORY(scheduler, simulate, "basic scheduler");

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
  auto names = mp::apply_visitor<mp::GetNames, SchedulerTypes>().result;
  BOOST_ASSERT(std::unique(names.begin(), names.end()) == names.end() && "Scheduler names must be unique");
  return names;
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
  case Type::DYNAMIC:
    // Set watchpoints so simulation stops when any task is finished
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

  _dump_state(start, config["output"].as<std::string>());
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


void Scheduler::_dump_state(const double start_time, const std::string& target_file) {
  using namespace rapidjson;
  Document jresult;
  jresult.SetObject();

  Value jtasks(kArrayType);
  jtasks.Reserve(_simulator->get_tasks().size(), jresult.GetAllocator());
  for (auto task: _simulator->get_tasks()) {
    auto task_kind = SD_task_get_kind(task);

    Value jtask(kObjectType);
    jtask.AddMember("name", Value().SetString(SD_task_get_name(task), jresult.GetAllocator()), jresult.GetAllocator());
    jtask.AddMember("type", Value().SetString(task_kind == SD_TASK_COMM_E2E ? "comm" : "comp", jresult.GetAllocator()), jresult.GetAllocator());
    jtask.AddMember("start", SD_task_get_start_time(task) - start_time, jresult.GetAllocator());
    jtask.AddMember("end", SD_task_get_finish_time(task) - start_time, jresult.GetAllocator());
    jtask.AddMember("amount", SD_task_get_amount(task), jresult.GetAllocator());

    Value jwslist(rapidjson::kArrayType);
    std::vector<SD_workstation_t> workstations{SD_task_get_workstation_list(task), SD_task_get_workstation_list(task) + SD_task_get_workstation_count(task)};
    for (auto ws: workstations) {
      jwslist.PushBack(Value().SetString(SD_workstation_get_name(ws), jresult.GetAllocator()), jresult.GetAllocator());
    }
    jtask.AddMember("hosts", jwslist, jresult.GetAllocator());

    jtasks.PushBack(jtask.Move(), jresult.GetAllocator());
  }
  jresult.AddMember("tasks", jtasks.Move(), jresult.GetAllocator());


  Value jhosts(kArrayType);
  jhosts.Reserve(_simulator->get_workstations().size(), jresult.GetAllocator());
  for (auto ws: _simulator->get_workstations()) {
    Value jhost(kObjectType);
    jhost.AddMember("name", Value().SetString(SD_workstation_get_name(ws), jresult.GetAllocator()), jresult.GetAllocator());
    jhost.AddMember("power", SD_workstation_get_power(ws), jresult.GetAllocator());
    jhost.AddMember("cores", SD_workstation_get_cores(ws), jresult.GetAllocator());

    jhosts.PushBack(jhost.Move(), jresult.GetAllocator());
  }
  jresult.AddMember("hosts", jhosts.Move(), jresult.GetAllocator());

  StringBuffer buffer;
  PrettyWriter<StringBuffer> writer(buffer);
  jresult.Accept(writer);

  XBT_DEBUG("Result:\n%s", buffer.GetString());

  if (!target_file.empty()) {
    FILE* const file = fopen(target_file.c_str(), "wb");
    if (!file) {
      perror("Failed to open output file");
      throw std::runtime_error("Failed to write output");
    }
    fwrite(buffer.GetString(), buffer.GetSize(), sizeof(StringBuffer::Ch), file);
    fwrite("\n", 1, 1, file);
    fclose(file);
  }
}

}
