project(escs C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")

file(GLOB ESCS_SRC
        "*.c"
        )

set(ESCS_PROJECT_NAME escs)
add_executable(${ESCS_PROJECT_NAME} ${ESCS_SRC})
aws_set_common_properties(${ESCS_PROJECT_NAME})


target_include_directories(${ESCS_PROJECT_NAME} PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(${ESCS_PROJECT_NAME} aws-c-event-stream)

if (BUILD_SHARED_LIBS AND NOT WIN32)
    message(INFO " escs will be built with shared libs, but you may need to set LD_LIBRARY_PATH=${CMAKE_INSTALL_PREFIX}/lib to run the application")
endif()

install(TARGETS ${ESCS_PROJECT_NAME}
        EXPORT ${ESCS_PROJECT_NAME}-targets
        COMPONENT Runtime
        RUNTIME
        DESTINATION bin
        COMPONENT Runtime)
