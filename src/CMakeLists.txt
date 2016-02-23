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
set_if_not_set(SimGrid_SEARCH_PATH "${CMAKE_SOURCE_DIR}/../opt/SimGrid")
set_if_not_set(RapidJSON_SEARCH_PATH "${CMAKE_SOURCE_DIR}/../opt/rapidjson")

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
message(STATUS "Looking for RapidJSON...")
find_path(RapidJSON_ROOT "include/rapidjson/rapidjson.h" PATHS "${RapidJSON_SEARCH_PATH}" NO_DEFAULT_PATH)
if (NOT RapidJSON_ROOT)
  message(FATAL_ERROR "Failed to find RapidJSON. Search path: ${RapidJSON_SEARCH_PATH}")
endif ()
message(STATUS "RapidJSON root: ${RapidJSON_ROOT}")
set(RapidJSON_INCLUDE_DIRS "${RapidJSON_ROOT}/include")
message(STATUS)

message(STATUS)
message(STATUS "Looking for Boost...")
find_package(Boost REQUIRED COMPONENTS program_options filesystem system)
message(STATUS)
# --------------------------

# 3. Subprojects
#
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

add_subdirectory(simulate)
add_subdirectory(plot)
# --------------------------

# 4. Tests
#
enable_testing()
set(TEST_DATA_ROOT "${CMAKE_SOURCE_DIR}/test_data")

add_test(NAME basic_list_heuristics COMMAND "$<TARGET_FILE:simulate>" --algorithm=list_heuristic "${TEST_DATA_ROOT}/pl_4hosts.xml" "${TEST_DATA_ROOT}/basic_graph.dot" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_test(NAME basic_random          COMMAND "$<TARGET_FILE:simulate>" --algorithm=random --seed=42 "${TEST_DATA_ROOT}/pl_4hosts.xml" "${TEST_DATA_ROOT}/basic_graph.dot" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
add_test(NAME basic_round_robin     COMMAND "$<TARGET_FILE:simulate>" --algorithm=round_robin "${TEST_DATA_ROOT}/pl_4hosts.xml" "${TEST_DATA_ROOT}/basic_graph.dot" WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}")
# --------------------------
