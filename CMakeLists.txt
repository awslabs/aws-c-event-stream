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
project (aws-c-event-stream)

if(NOT MSVC)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
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

find_package(aws-c-common)
find_package(aws-checksums)

add_library(${CMAKE_PROJECT_NAME} ${EVENT_STREAM_HEADERS} ${EVENT_STREAM_SRC})

if(BUILD_SHARED_LIBS)
    if(WIN32)
        target_compile_definitions(${CMAKE_PROJECT_NAME} PUBLIC -DUSE_IMPORT_EXPORT)
		target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE -DAWS_EVENT_STREAM_EXPORTS)
	endif()
endif()

set_target_properties(${CMAKE_PROJECT_NAME} PROPERTIES LINKER_LANGUAGE C)
set(CMAKE_C_FLAGS_DEBUGOPT "")
set_property(TARGET ${CMAKE_PROJECT_NAME} PROPERTY C_STANDARD 99)

if(BUILD_JNI_BINDINGS)
    find_package(JNI)
    include_directories(${JNI_INCLUDE_DIRS})
	set(PLATFORM_LIBS ${PLATFORM_LIBS} ${JNI_LIBRARIES})
	target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE -DBUILD_JNI_BINDINGS)
endif()

if(CMAKE_BUILD_TYPE STREQUAL "" OR CMAKE_BUILD_TYPE MATCHES DEBUG)
    target_compile_definitions(${CMAKE_PROJECT_NAME} PRIVATE -DDEBUG_BUILD)
endif()

target_include_directories(${CMAKE_PROJECT_NAME} PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>)

target_link_libraries(${CMAKE_PROJECT_NAME} aws-c-common aws-checksums)

install (FILES ${AWS_EVENT_STREAM_HEADERS} DESTINATION "include/aws/event-stream")
install (
         TARGETS ${PROJECT_NAME} EXPORT  ${PROJECT_NAME}-config
         ARCHIVE DESTINATION lib
         LIBRARY DESTINATION lib
         COMPONENT library     
)

export(TARGETS ${CMAKE_PROJECT_NAME} FILE "${CMAKE_PROJECT_NAME}-config.cmake")
install(EXPORT ${CMAKE_PROJECT_NAME}-config DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/${CMAKE_PROJECT_NAME}/cmake/")

include(CTest)
add_subdirectory(tests)
add_subdirectory(bin)
