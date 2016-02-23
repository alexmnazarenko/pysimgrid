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
