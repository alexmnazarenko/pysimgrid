// (c) DATADVANCE 2016

#include "simulator.hpp"
#include "scheduler.hpp"

#include <rapidjson/document.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <stdexcept>


#define SIMULATOR_RAPIDJSON_CHECK_TYPE(jsonvalue, name, type)     \
  do {                                                            \
    if (!(jsonvalue).Is##type()) {                                \
      std::runtime_error("node " #name "must be of type " #type); \
    }                                                             \
  } while (0);                                                    \


namespace {
  const rapidjson::Value& getJSONValue(const rapidjson::Value& root, const char* const name) {
    if (!root.HasMember(name)) {
      throw std::runtime_error("no required member not found");
    }
    return root[name];
  }
}

XBT_LOG_NEW_DEFAULT_CATEGORY(simulator, "darunner simulator");

namespace bfs = boost::filesystem;

namespace darunner {

const std::string Simulator::ROOT_TASK = "root";
const std::string Simulator::END_TASK = "end";


Simulator::Simulator(const std::string& platform_path, const std::string& tasks_path, TaskFormat task_format) {
  if (!bfs::is_regular_file(platform_path)) {
    throw std::runtime_error("platform configuration file does not exist");
  }
  if (!bfs::is_regular_file(tasks_path)) {
    throw std::runtime_error("platform configuration file does not exist");
  }
  // Load platform info and store it for convenience
  XBT_INFO("Loading platform from '%s'", platform_path.c_str());
  SD_create_environment(platform_path.c_str());
  _workstations.assign(SD_workstation_get_list(), SD_workstation_get_list() + SD_workstation_get_number());
  _links.assign(SD_link_get_list(), SD_link_get_list() + SD_link_get_number());
  // Attach data to workstations
  for (auto& workstation: _workstations) {
    SD_workstation_set_data(workstation, new WorkstationData);
  }

  switch (task_format) {
  case TASK_FORMAT_DOT:
    _load_tasks_dot(tasks_path);
    break;
  case TASK_FORMAT_JSON:
    throw std::runtime_error("json task format not implemented yet");
    _load_tasks_json(tasks_path);
    break;
  default:
    throw std::runtime_error("unknown task format");
  }

  for (auto& task: _tasks) {
    SD_task_set_data(task, new TaskData);
  }
}


Simulator::~Simulator() noexcept {
  for (auto& task: _tasks) {
    TaskData* const data = reinterpret_cast<TaskData*>(SD_task_get_data(task));
    delete data;
    SD_task_destroy(task);
  }
  for (auto& workstation: _workstations) {
    WorkstationData* const data = reinterpret_cast<WorkstationData*>(SD_workstation_get_data(workstation));
    delete data;
  }
}


Simulator::WorkstationData* Simulator::workstation_get_data(SD_workstation_t const workstation) {
  void* const data = SD_workstation_get_data(workstation);
  BOOST_ASSERT(data && "no attached data on workstation");
  return reinterpret_cast<WorkstationData*>(data);
}


Simulator::TaskData* Simulator::task_get_data(SD_task_t const task) {
  void* const data = SD_task_get_data(task);
  BOOST_ASSERT(data && "no attached data on task");
  return reinterpret_cast<TaskData*>(data);
}


SD_task_t Simulator::task_by_name(const std::string& name) {
  for (auto& task: _tasks) {
    if (SD_task_get_name(task) == name) {
      return task;
    }
  }
  throw std::runtime_error("task with a given name does not exist");
}


void Simulator::simulate(Scheduler::Algorithm schedule) {
  std::unique_ptr<Scheduler> scheduler = Scheduler::create(schedule, *this);
  switch (scheduler->type()) {
  case Scheduler::Type::STATIC:
    scheduler->schedule();
    SD_simulate(-1);
    break;
  case Scheduler::Type::DYNAMIC:
    for (auto task: _tasks) {
      if (SD_task_get_kind(task) == SD_TASK_COMP_SEQ) {
        SD_task_watch(task, SD_DONE);
      }
    }
    scheduler->schedule();
    while (!xbt_dynar_is_empty(SD_simulate(-1))) {
      scheduler->schedule();
    }
    break;
  default:
    throw std::runtime_error("unknown scheduler type");
  }
  XBT_INFO("Simulation time: %f seconds\n", SD_get_clock());
}


void Simulator::_load_tasks_dot(const std::string& tasks_path) {
  xbt_dynar_t task_array = SD_dotload(tasks_path.c_str());
  unsigned idx;
  SD_task_t element;
  xbt_dynar_foreach(task_array, idx, element) {
    _tasks.push_back(element);
  }
  xbt_dynar_free_container(&task_array);
}


void Simulator::_load_tasks_json(const std::string& tasks_path) {
  // TODO: finalize when extended load will be required
  std::ifstream file_stream(tasks_path);
  if (!file_stream.is_open()) {
    throw std::runtime_error("failed to open tasks configration file");
  }
  std::string data(std::istreambuf_iterator<char>{file_stream}, std::istreambuf_iterator<char>{});
  rapidjson::Document doc;
  doc.Parse(data.c_str());
  if (doc.HasParseError()) {
    throw std::runtime_error("failed to parse tasks json");
  }
  SIMULATOR_RAPIDJSON_CHECK_TYPE(doc, "<root>", Object);
  const rapidjson::Value& tasks = getJSONValue(doc, "tasks");
  const rapidjson::Value& links = getJSONValue(doc, "links");
  SIMULATOR_RAPIDJSON_CHECK_TYPE(tasks, "tasks", Array);
  SIMULATOR_RAPIDJSON_CHECK_TYPE(links, "links", Array);
  for (rapidjson::Value::ConstValueIterator it = tasks.Begin(); it != tasks.End(); ++it) {
    const rapidjson::Value& task = *it;
    SIMULATOR_RAPIDJSON_CHECK_TYPE(task, "task", Object);
    const std::string name(task["name"].GetString(), task["name"].GetStringLength());
    const double flops = task["size"].GetDouble();
  }
}

}
