// (c) DATADVANCE 2016

#include <string>
#include <memory>

namespace darunner {

class Scheduler;

class Simulator {
public:
  Simulator(const std::string& platform_path, const std::string& tasks_path);
  ~Simulator() noexcept;

  /**
   * Run SimGrid simulation, probably in multiple steps if dynamic scheduling is used.
   */
  void simulate(Scheduler& scheduler);

private:
  // Shouldn't be ever copied
  Simulator(const Simulator&);

  void _load_tasks_json(const std::string& tasks_path);
};

}
