// (c) DATADVANCE 2016

#include "simulator.hpp"
#include "scheduler.hpp"
// weirdass simgrid miserably fails if platf is not included first. it's also included in simdag.h, but order seems to be incorrect.
#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <functional>
#include <memory>

namespace po = boost::program_options;

XBT_LOG_NEW_DEFAULT_CATEGORY(darunner, "darunner tool root");

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
    Simulator simulator(config["platform"].as<std::string>(), config["tasks"].as<std::string>());
    simulator.simulate(Scheduler::Algorithm::GREEDY);
    return;

    const std::string tasks_path = config["tasks"].as<std::string>();
    XBT_INFO("Loading graph from '%s'", tasks_path.c_str());
    auto graph = xbt_to_vector<SD_task_t>(SD_dotload(tasks_path.c_str()), SD_task_destroy);

    const SD_workstation_t* workstations = SD_workstation_get_list();
    const unsigned nworkstations = SD_workstation_get_number();
    // Dump platform configuration
    for (unsigned wsIdx = 0; wsIdx < nworkstations; ++wsIdx) {
      SD_workstation_dump(workstations[wsIdx]);
    }
    const SD_link_t* links = SD_link_get_list();
    const unsigned nlinks = SD_link_get_number();
    for (unsigned lIdx = 0; lIdx < nlinks; ++lIdx) {
      XBT_INFO("Link:");
      XBT_INFO("  %s: %f", SD_link_get_name(links[lIdx]), SD_link_get_current_bandwidth(links[lIdx]));
    }

    // Schedule tasks
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



    while (!xbt_dynar_is_empty(SD_simulate(-1))) {
    }
    XBT_INFO("Simulation time: %f seconds\n", SD_get_clock());

    // Sort results
    std::multimap<double, SD_task_t> results;
    for (auto& task: graph) {
      const double start = SD_task_get_start_time(task.get());
      results.insert({start, task.get()});
    }

    // Dump results
    // TODO: format?
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

  }
}

int main(int argc, char* argv[]) {
  // 1.
  // Parse command line options
  po::options_description cmdline_desc("Allowed options");
  cmdline_desc.add_options()
      ("help", "produce help message")
      ("help-simgrid", "show simgrid config parameters")
      ("tasks", po::value<std::string>()->required(), "path to task graph definition in .dot format")
      ("platform", po::value<std::string>()->required(), "path to platform definition in .xml format")
      ("simgrid", po::value<std::vector<std::string>>(), "simgrid config parameters; may be passed multiple times")
  ;
  darunner::Scheduler::register_options(cmdline_desc);

  po::positional_options_description cmdline_positional;
  cmdline_positional.add("tasks", 1);
  cmdline_positional.add("platform", 1);

  po::variables_map config;
  try {
    po::store(po::command_line_parser(argc, argv).options(cmdline_desc).positional(cmdline_positional).run(), config);
    po::notify(config);
  } catch (std::exception& e) {
    std::cout << "Usage: darunner [options] <task_graph> <platform_description>\n" << std::endl;
    std::cout << e.what() << "\n" << std::endl;
    std::cout << cmdline_desc << std::endl;
    return 1;
  }

  // -------------------------

  // 2.
  // Init SimDAG library and ensure it's cleanup
  //
  // Don't feed it with command line, so it doesn't mess up our own cmdline syntax.
  int fakeArgc = config.count("help-simgrid") ? 2 : 1;
  const char* fakeArgv[] = {
    argv[0],
    "--help"
  };
  SD_init(&fakeArgc, const_cast<char**>(fakeArgv));
  darunner::OnScopeExit guard(SD_exit);
  (void) guard;

  if (config.count("simgrid")) {
    auto simgrid_options = config["simgrid"].as<std::vector<std::string>>();
    for (const auto& cfg_param: simgrid_options) {
      const auto delim_pos = cfg_param.find(":");
      if (delim_pos == std::string::npos) {
        throw std::runtime_error("malformed simgrid config parameter");
      }
      const auto name = cfg_param.substr(0, delim_pos);
      const auto value = cfg_param.substr(delim_pos + 1);
      SD_config(name.c_str(), value.c_str());
    }
  }
  // -------------------------


  // 3.
  // Go
  try {
    darunner::execute(config);
  } catch (std::exception& e) {
    std::cout << "----------------------------------" << std::endl;
    std::cout << "\nSimulation failed\n" << std::endl;
    std::cout << "  Error: " << e.what() << std::endl;
    std::cout << "----------------------------------" << std::endl;
  }

  return 0;
}
