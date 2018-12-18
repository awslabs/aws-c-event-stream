# Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
cmake_minimum_required (VERSION 3.1)
project (aws-c-event-stream C)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_INSTALL_PREFIX}/lib/cmake")
include(AwsCFlags)
include(AwsSanitizers)
include(CheckCCompilerFlag)

if(NOT MSVC)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
endif()

file(GLOB AWS_EVENT_STREAM_HEADERS
     "include/aws/event-stream/*.h"
)

file(GLOB AWS_EVENT_STREAM_PRIV_HEADERS
     "include/aws/event-stream/private/*.h"
)

file(GLOB AWS_EVENT_STREAM_SRC
    "source/*.c"
)

if(WIN32)
     if(MSVC)
         source_group("Header Files\\aws\\event-stream" FILES ${AWS_EVENT_STREAM_HEADERS})
         source_group("Source Files" FILES ${AWS_EVENT_STREAM_SRC})
     endif()
endif()

file(GLOB EVENT_STREAM_HEADERS
    ${AWS_EVENT_STREAM_HEADERS}
)

file(GLOB EVENT_STREAM_SRC
    ${AWS_EVENT_STREAM_SRC}
)

add_library(${CMAKE_PROJECT_NAME} ${EVENT_STREAM_SRC})
aws_set_common_properties(${CMAKE_PROJECT_NAME})
aws_add_sanitizers(${CMAKE_PROJECT_NAME})

target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

if(BUILD_SHARED_LIBS)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PUBLIC -DAWS_EVENT_STREAM_USE_IMPORT_EXPORT)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE -DAWS_EVENT_STREAM_EXPORTS)
endif()

find_package(aws-c-common REQUIRED)
find_package(aws-checksums REQUIRED)

target_link_libraries(${CMAKE_PROJECT_NAME} PUBLIC AWS::aws-c-common AWS::aws-checksums)

# installation & packaging
set(LIBRARY_DIRECTORY ${CMAKE_INSTALL_PREFIX}/lib)

install(FILES ${AWS_EVENT_STREAM_HEADERS} DESTINATION "include/aws/event-stream")
install(TARGETS ${CMAKE_PROJECT_NAME}
    EXPORT ${CMAKE_PROJECT_NAME}-targets
    ARCHIVE DESTINATION ${LIBRARY_DIRECTORY}
    LIBRARY DESTINATION ${LIBRARY_DIRECTORY}
    RUNTIME DESTINATION lib)

install(EXPORT "${CMAKE_PROJECT_NAME}-targets"
    DESTINATION "${LIBRARY_DIRECTORY}/${CMAKE_PROJECT_NAME}/cmake/"
    NAMESPACE AWS::)

configure_file("cmake/${CMAKE_PROJECT_NAME}-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}-config.cmake"
    @ONLY)

install(FILES "${CMAKE_CURRENT_BINARY_DIR}/${CMAKE_PROJECT_NAME}-config.cmake"
    DESTINATION "${LIBRARY_DIRECTORY}/${CMAKE_PROJECT_NAME}/cmake/")


include(CTest)
enable_testing()
add_subdirectory(tests)

if(NOT MSVC)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif()

add_subdirectory(bin)
