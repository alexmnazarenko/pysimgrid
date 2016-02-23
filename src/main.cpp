// A. Nazarenko 2016

#include "simulator.hpp"
#include "scheduler.hpp"
// weird*** simgrid miserably fails if platf is not included first. it's also included in simdag.h, but order seems to be incorrect.
#include "simgrid/platf.h"
#include "simgrid/simdag.h"

#include <boost/program_options.hpp>
#include <iostream>
#include <functional>
#include <memory>

namespace po = boost::program_options;

XBT_LOG_NEW_DEFAULT_CATEGORY(simulate, "simulate tool root");

namespace simulate {

/**
 * Simplistic scope guard. Single-use only )
 */
class OnScopeExit {
public:
  OnScopeExit(const std::function<void()>& e) : _exit(e) {}
  ~OnScopeExit() throw() { _exit(); }
private:
  std::function<void()> _exit;
};


void main(const po::variables_map& config) {
  if (config.count("verbose")) {
    xbt_log_control_set("simulate.thres:TRACE");
  }

  SimulatorState simulator_state(config["platform"].as<std::string>(), config["tasks"].as<std::string>());
  const auto scheduler = Scheduler::create(config["algorithm"].as<std::string>());
  scheduler->run(simulator_state, config);
}

}

int main(int argc, char* argv[]) {
  // 1.
  // Parse command line options
  po::options_description cmdline_desc("Allowed options");
  cmdline_desc.add_options()
      ("help", "produce help message")
      ("help-simgrid", "show simgrid config parameters")
      ("verbose,-v", "enable verbose output")
      ("tasks", po::value<std::string>()->required(), "path to task graph definition in .dot format")
      ("platform", po::value<std::string>()->required(), "path to platform definition in .xml format")
      ("output,o", po::value<std::string>()->default_value(""), "path to output file")
      ("algorithm,a", po::value<std::string>()->default_value("list_heuristic"), "scheduling algorithm to use")
      ("simgrid", po::value<std::vector<std::string>>(), "simgrid config parameters; may be passed multiple times")
  ;
  simulate::Scheduler::register_options(cmdline_desc);

  po::positional_options_description cmdline_positional;
  cmdline_positional.add("platform", 1);
  cmdline_positional.add("tasks", 1);

  po::variables_map config;
  try {
    po::store(po::command_line_parser(argc, argv).options(cmdline_desc).positional(cmdline_positional).run(), config);
    po::notify(config);

    const auto algorithms = simulate::Scheduler::names();
    if (std::count(algorithms.begin(), algorithms.end(), config["algorithm"].as<std::string>()) != 1) {
      throw std::runtime_error("unknown scheduling algorithm requested");
    }
  } catch (std::exception& e) {
    std::cout << "Usage: simulate [options] <task_graph> <platform_description>\n" << std::endl;
    std::cout << e.what() << "\n" << std::endl;
    std::cout << cmdline_desc << std::endl;
    std::cout << "Available algorithms:" << std::endl;
    for (const auto& scheduler: simulate::Scheduler::names()) {
      std::cout << "  " << scheduler << std::endl;
    }
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
  simulate::OnScopeExit guard(SD_exit);
  (void) guard; // supress 'unused' warning

  if (config.count("simgrid")) {
    auto simgrid_options = config["simgrid"].as<std::vector<std::string>>();
    for (const auto& cfg_param: simgrid_options) {
      const auto delim_pos = cfg_param.find(":");
      if (delim_pos == std::string::npos) {
        throw std::runtime_error("malformed simgrid config parameter");
      }
      const auto name = cfg_param.substr(0, delim_pos);
      const auto value = cfg_param.substr(delim_pos + 1);

      xbt_ex_t exception;
      TRY {
        SD_config(name.c_str(), value.c_str());
      } CATCH (exception) {
        std::cout << "Failed to set SimGrid parameter '" << name << "': " << exception.msg << std::endl;
      }
    }
  }
  // -------------------------


  // 3.
  // Go
  try {
    simulate::main(config);
  } catch (std::exception& e) {
    std::cout << "----------------------------------" << std::endl;
    std::cout << "\nSimulation failed\n" << std::endl;
    std::cout << "  Error: " << e.what() << std::endl;
    std::cout << "----------------------------------" << std::endl;
  }

  return 0;
}
