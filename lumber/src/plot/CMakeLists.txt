add_custom_command(OUTPUT "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/plot.py"
                   COMMAND "${CMAKE_COMMAND}" -E copy_if_different "${CMAKE_CURRENT_SOURCE_DIR}/plot.py" "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/plot.py"
                   DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/plot.py"
                   COMMENT "Exporting python file plot.py")

add_custom_target(export_plot ALL DEPENDS "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/plot.py")
