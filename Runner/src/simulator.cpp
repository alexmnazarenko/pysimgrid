// (c) DATADVANCE 2016

#include "simulator.hpp"
#include "simgrid/platf.h"
#include "simgrid/simdag.h"

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

Simulator::Simulator(const std::string& platform_path, const std::string& tasks_path) {
  if (!bfs::is_regular_file(platform_path)) {
    throw std::runtime_error("platform configuration file does not exist");
  }
  if (!bfs::is_regular_file(tasks_path)) {
    throw std::runtime_error("platform configuration file does not exist");
  }
  XBT_INFO("Loading platform from '%s'", platform_path.c_str());
  SD_create_environment(platform_path.c_str());
  _load_tasks_json(tasks_path);
}

Simulator::~Simulator() noexcept {}

void Simulator::simulate(Scheduler& scheduler) {

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
