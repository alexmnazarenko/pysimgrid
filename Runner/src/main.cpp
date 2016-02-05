// (c) DATADVANCE 2016

// weirdass simgrid miserably fails if platf is not included first. it's also included in simdag.h, but order seems to be incorrect.
#include "simgrid/platf.h"
#include "simgrid/simdag.h"
#include <boost/program_options.hpp>
#include <iostream>
#include <functional>
#include <memory>

namespace po = boost::program_options;

XBT_LOG_NEW_DEFAULT_CATEGORY(DARUNNER, "darunner tool root logger");

namespace darunner {
  // Simplistic scope guard. Single-use only )
  class OnScopeExit {
  public:
    OnScopeExit(const std::function<void()>& e) : _exit(e) {}
    ~OnScopeExit() throw() { _exit(); }
  private:
    std::function<void()> _exit;
  };

  template <class T>
  using Handle = std::shared_ptr<class std::remove_pointer<T>::type>;

  template<class T>
  std::vector<Handle<T>> xbt_to_vector(xbt_dynar_t array, std::function<void(T)> dealloc) {
    if (!array) {
      throw std::runtime_error("cannot convert empty array");
    }
    std::vector<Handle<T>> result;
    unsigned int cpt;
    T element;
    xbt_dynar_foreach(array, cpt, element) {
      result.push_back({element, dealloc});
    }
    xbt_dynar_free_container(&array);
    return result;
  }


  void execute(const po::variables_map& config) {

    const std::string tasks_path = config["tasks"].as<std::string>();
    XBT_INFO("Loading graph from '%s'", tasks_path.c_str());
    auto graph = xbt_to_vector<SD_task_t>(SD_dotload(tasks_path.c_str()), SD_task_destroy);

    const std::string network_path = config["network"].as<std::string>();
    XBT_INFO("Loading network from '%s'", network_path.c_str());
    SD_create_environment(network_path.c_str());

    const SD_workstation_t* workstations = SD_workstation_get_list();
    const unsigned nworkstations = SD_workstation_get_number();
    unsigned wsIdx = 0;
    for (auto& task: graph) {
      if (SD_task_get_kind(task.get()) == SD_TASK_COMP_SEQ) {
        unsigned scheduledTo = 0;
        if (SD_task_get_name(task.get()) != std::string("root") && SD_task_get_name(task.get()) != std::string("end")) {
          scheduledTo = wsIdx++ % nworkstations;
        }
        SD_task_schedulel(task.get(), 1, workstations[scheduledTo]);
        //SD_task_dump(task.get());
      }
    }

    for (unsigned wsIdx = 0; wsIdx < nworkstations; ++wsIdx) {
      SD_workstation_dump(workstations[wsIdx]);
    }

    while (!xbt_dynar_is_empty(SD_simulate(-1))) {
    }

    std::multimap<double, SD_task_t> results;
    for (auto& task: graph) {
      const double start = SD_task_get_start_time(task.get());
      results.insert({start, task.get()});
    }

    for (auto& result: results) {
      const SD_task_t task = result.second;
      const double start = SD_task_get_start_time(task);
      const double end = SD_task_get_finish_time(task);
      const unsigned wsCount = SD_task_get_workstation_count(task);
      const SD_workstation_t* ws = SD_task_get_workstation_list(task);
      std::cout << SD_task_get_name(task) << ": " << start << " " << (end - start) << std::endl;
      std::cout << " ";
      for (unsigned wsIdx = 0; wsIdx < wsCount; ++wsIdx) {
        std::cout << " " << SD_workstation_get_name(ws[wsIdx]);
      }
      std::cout << std::endl;
    }

    XBT_INFO("Simulation time: %f seconds\n", SD_get_clock());
  }
}

int main(int argc, char* argv[]) {
  // 1.
  // Init SimDAG library and ensure it's cleanup
  // Don't feed it with command line to avoid strange interactions
  int fakeArgc = 1;
  SD_init(&argc, argv);
  darunner::OnScopeExit guard(SD_exit);
  (void) guard;
  // -------------------------

  // 2.
  // Parse command line options
  po::options_description cmdlineDesc("Allowed options");
  cmdlineDesc.add_options()
      ("help", "produce help message")
      ("tasks", po::value<std::string>()->required(), "path to task graph definition in .dot format")
      ("network", po::value<std::string>()->required(), "path to network definition in .xml format")
  ;
  po::positional_options_description cmdlinePositional;
  cmdlinePositional.add("tasks", 1);
  cmdlinePositional.add("network", 1);

  po::variables_map config;
  try {
    po::store(po::command_line_parser(argc, argv).options(cmdlineDesc).positional(cmdlinePositional).run(), config);
    po::notify(config);
  } catch (std::exception& e) {
    std::cout << "Usage: darunner [options] <task_graph> <network_description>\n" << std::endl;
    std::cout << e.what() << "\n" << std::endl;
    std::cout << cmdlineDesc << "\n";
    return 1;
  }
  // -------------------------

  // 3.
  // Go
  darunner::execute(config);

  return 0;
}
