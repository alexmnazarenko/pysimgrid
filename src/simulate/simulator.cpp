// A. Nazarenko 2016

#include "simulator.hpp"
#include "scheduler.hpp"

#include <rapidjson/document.h>
#include <boost/filesystem.hpp>
#include <fstream>
#include <stdexcept>

XBT_LOG_EXTERNAL_CATEGORY(simulate);
XBT_LOG_NEW_DEFAULT_SUBCATEGORY(state, simulate, "simulator state");

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

namespace bfs = boost::filesystem;

namespace simulate {

const std::string SimulatorState::ROOT_TASK = "root";
const std::string SimulatorState::END_TASK = "end";


SimulatorState::SimulatorState(const std::string& platform_path, const std::string& tasks_path, TaskFormat task_format) {
  // Check that required paths exist
  if (!bfs::is_regular_file(platform_path)) {
    throw std::runtime_error("platform configuration file does not exist");
  }
  if (!bfs::is_regular_file(tasks_path)) {
    throw std::runtime_error("tasks description file does not exist");
  }
  // Select loader for task description
  if (task_format == TaskFormat::AUTO) {
    const auto task_ext = bfs::path(tasks_path).extension().string();
    const std::map<std::string, TaskFormat> ext_map = {
      {".dot", TaskFormat::DOT},
      {".dax", TaskFormat::DAX},
      {".xml", TaskFormat::DAX},
      {".json", TaskFormat::JSON},
    };
    if (!ext_map.count(task_ext)) {
      throw std::runtime_error("unable to determine task description file format");
    }
    task_format = ext_map.at(task_ext);
    XBT_INFO("Autodetected task file type by extension ('%s')", task_ext.c_str());
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

  // Load tasks description
  switch (task_format) {
  case TaskFormat::DOT:
    XBT_INFO("Loading tasks definition (Pegasus DAX format, '%s')", tasks_path.c_str());
    _load_tasks_dot(tasks_path);
    break;
  case TaskFormat::DAX:
    XBT_INFO("Loading tasks definition (SimGrid dot format, path '%s')", tasks_path.c_str());
    _load_tasks_dax(tasks_path);
    break;
  case TaskFormat::JSON:
    throw std::runtime_error("json task format not implemented yet");
    XBT_INFO("Loading tasks definition (Custom json format, '%s')", tasks_path.c_str());
    _load_tasks_json(tasks_path);
    break;
  default:
    throw std::runtime_error("unknown task format");
  }
  // Attach data to tasks
  for (auto& task: _tasks) {
    SD_task_set_data(task, new TaskData);
  }
}


SimulatorState::~SimulatorState() noexcept {
  for (auto& task: _tasks) {
    TaskData* const data = &task_data(task);
    delete data;
    SD_task_destroy(task);
  }
  for (auto& workstation: _workstations) {
    WorkstationData* const data = reinterpret_cast<WorkstationData*>(SD_workstation_get_data(workstation));
    delete data;
  }
}


SimulatorState::WorkstationData& SimulatorState::workstation_data(SD_workstation_t const workstation) {
  void* const data = SD_workstation_get_data(workstation);
  BOOST_ASSERT(data && "no attached data on workstation");
  return *reinterpret_cast<WorkstationData*>(data);
}


SimulatorState::TaskData& SimulatorState::task_data(SD_task_t const task) {
  void* const data = SD_task_get_data(task);
  BOOST_ASSERT(data && "no attached data on task");
  return *reinterpret_cast<TaskData*>(data);
}


SD_task_t SimulatorState::task_by_name(const std::string& name) {
  for (auto& task: _tasks) {
    if (SD_task_get_name(task) == name) {
      return task;
    }
  }
  throw std::runtime_error("task with a given name does not exist");
}


bool SimulatorState::simulate() {
  return !xbt_dynar_is_empty(SD_simulate(-1));
}


void SimulatorState::_load_tasks_dot(const std::string& tasks_path) {
  xbt_dynar_t task_array = SD_dotload(tasks_path.c_str());
  unsigned idx;
  SD_task_t element;
  xbt_dynar_foreach(task_array, idx, element) {
    _tasks.push_back(element);
  }
  xbt_dynar_free_container(&task_array);
}


void SimulatorState::_load_tasks_dax(const std::string& tasks_path) {
  xbt_dynar_t task_array = SD_daxload(tasks_path.c_str());
  unsigned idx;
  SD_task_t element;
  xbt_dynar_foreach(task_array, idx, element) {
    _tasks.push_back(element);
  }
  xbt_dynar_free_container(&task_array);
}


void SimulatorState::_load_tasks_json(const std::string& tasks_path) {
  // TODO: finalize when extended load will be required
  // TODO: maybe use 'trunk' rapidjson for jsonschema support? validation seems to be even more pain than usual.
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
    (void)name;
    (void)flops;
  }
}

}
