cmake_minimum_required(VERSION 2.8)

# 0. Utility defintions
#
macro(set_if_not_set VAR VAL)
  if (NOT DEFINED ${VAR})
    set(${VAR} ${VAL})
    message(STATUS "Using default ${VAR} = ${VAL}")
  else ()
    message(STATUS "Using externally defined ${VAR} = ${VAL}")
  endif ()
endmacro()
# --------------------------

# 1. Project header
#
project(DARunner CXX)

message(STATUS "
  DARunner SimGrid frontend.

  Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}
")
# --------------------------


# 2. Dependency search
#
set_if_not_set(SIMGRID_SEARCH_PATH "${CMAKE_SOURCE_DIR}/../../SimGrid")

message(STATUS)
message(STATUS "Looking for SimGrid...")
find_path(SIMGRID_ROOT "include/simgrid.h" PATHS "${SIMGRID_SEARCH_PATH}" NO_DEFAULT_PATH)
if (NOT SIMGRID_ROOT)
  message(FATAL_ERROR "Failed to find SimGrid. Search path: ${SIMGRID_SEARCH_PATH}")
endif ()
message(STATUS "SimGrid root: ${SIMGRID_ROOT}")
find_library(SIMGRID_LIBRARIES "simgrid" PATHS "${SIMGRID_ROOT}/lib" NO_DEFAULT_PATH)
message(STATUS "SimGrid library: ${SIMGRID_LIBRARIES}")
set(SIMGRID_INCLUDE_DIRS "${SIMGRID_ROOT}/include")
message(STATUS)

message(STATUS)
message(STATUS "Looking for Boost...")
find_package(Boost REQUIRED COMPONENTS program_options filesystem system)
message(STATUS)

message(STATUS)
message(STATUS "Using bundled RapidJSON...")
set(RapidJSON_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/rapidjson-1.0.2/include")
message(STATUS)

message(STATUS)
message(STATUS "Collecting sources...")
file(GLOB SOURCES *.cpp *.hpp)
foreach(SOURCE ${SOURCES})
  message(STATUS "  source: ${SOURCE}")
endforeach()
message(STATUS)
# --------------------------

# 3. Main binary configuration
#
include_directories(${SIMGRID_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${RapidJSON_INCLUDE_DIRS})
# SimGrid required c++11 usage
set(CMAKE_CXX_FLAGS "-std=c++11")

add_executable(darunner ${SOURCES})
target_link_libraries(darunner ${SIMGRID_LIBRARIES})
target_link_libraries(darunner ${Boost_PROGRAM_OPTIONS_LIBRARIES} ${Boost_FILESYSTEM_LIBRARIES} ${Boost_SYSTEM_LIBRARIES})
# --------------------------

# 4. Tests definition
set(TEST_DATA_ROOT "${CMAKE_SOURCE_DIR}/test_data")
enable_testing()
add_test(NAME load_basic_graph COMMAND "$<TARGET_FILE:darunner>" "${TEST_DATA_ROOT}/basic_graph.json" "${TEST_DATA_ROOT}/pl_4hosts.xml" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
# --------------------------
