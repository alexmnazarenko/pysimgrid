cmake_minimum_required(VERSION 3.2)

# 0. Utility defintions
#
macro(set_if_not_set VAR VAL)
  if (NOT DEFINED ${VAR})
    set(${VAR} ${VAL})
    message(STATUS "Using default ${VAR} = ${VAL}")
  else ()
    message(STATUS "Using defined ${VAR} = ${VAL}")
  endif ()
endmacro()
# --------------------------

# 1. Project header
#
project(simulate CXX)

message(STATUS "
  simulate SimGrid frontend.

  Cmake version ${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION}
")

# --------------------------


# 2. Dependency search
#
set_if_not_set(SimGrid_SEARCH_PATH "${CMAKE_SOURCE_DIR}/../../SimGrid")

message(STATUS)
message(STATUS "Looking for SimGrid...")
find_path(SimGrid_ROOT "include/simgrid.h" PATHS "${SimGrid_SEARCH_PATH}" NO_DEFAULT_PATH)
if (NOT SimGrid_ROOT)
  message(FATAL_ERROR "Failed to find SimGrid. Search path: ${SimGrid_SEARCH_PATH}")
endif ()
message(STATUS "SimGrid root: ${SimGrid_ROOT}")
find_library(SimGrid_LIBRARIES "simgrid" PATHS "${SimGrid_ROOT}/lib" NO_DEFAULT_PATH)
message(STATUS "SimGrid library: ${SimGrid_LIBRARIES}")
set(SimGrid_INCLUDE_DIRS "${SimGrid_ROOT}/include")
message(STATUS)

message(STATUS)
message(STATUS "Looking for Boost...")
find_package(Boost REQUIRED COMPONENTS program_options filesystem system)
message(STATUS)

message(STATUS)
message(STATUS "Using bundled RapidJSON...")
set(RapidJSON_INCLUDE_DIRS "${CMAKE_SOURCE_DIR}/rapidjson-1.0.2/include")
message(STATUS)
# --------------------------

# 3. Main binary configuration
#
message(STATUS)
message(STATUS "Collecting sources...")
file(GLOB SOURCES *.cpp *.hpp)
foreach(SOURCE ${SOURCES})
  message(STATUS "  source: ${SOURCE}")
endforeach()
message(STATUS)

include_directories(${SimGrid_INCLUDE_DIRS})
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${RapidJSON_INCLUDE_DIRS})
# SimGrid for some reason requires c++11 usage
set(CMAKE_CXX_FLAGS "-std=c++11 -Wall")

add_executable(simulate ${SOURCES})
target_link_libraries(simulate ${SimGrid_LIBRARIES})
target_link_libraries(simulate ${Boost_PROGRAM_OPTIONS_LIBRARIES} ${Boost_FILESYSTEM_LIBRARIES} ${Boost_SYSTEM_LIBRARIES})

install(TARGETS simulate
        RUNTIME DESTINATION bin)
# --------------------------

# 4. Tests
#
enable_testing()
set(TEST_DATA_ROOT "${CMAKE_SOURCE_DIR}/test_data")

add_test(NAME basic_list_heuristics COMMAND "$<TARGET_FILE:simulate>" --algorithm=list_heuristic "${TEST_DATA_ROOT}/pl_4hosts.xml" "${TEST_DATA_ROOT}/basic_graph.dot" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_test(NAME basic_random          COMMAND "$<TARGET_FILE:simulate>" --algorithm=random --seed=42 "${TEST_DATA_ROOT}/pl_4hosts.xml" "${TEST_DATA_ROOT}/basic_graph.dot" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_test(NAME basic_round_robin     COMMAND "$<TARGET_FILE:simulate>" --algorithm=round_robin "${TEST_DATA_ROOT}/pl_4hosts.xml" "${TEST_DATA_ROOT}/basic_graph.dot" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
# --------------------------
