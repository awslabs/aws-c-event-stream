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
enable_testing()

file(GLOB TEST_SRC "run_tests.c")
file(GLOB TESTS ${TEST_SRC})

set(TEST_BINARY_NAME ${CMAKE_PROJECT_NAME}-tests)

add_executable(${TEST_BINARY_NAME} ${TESTS})
set_target_properties(${TEST_BINARY_NAME} PROPERTIES LINKER_LANGUAGE C)
set_target_properties(${TEST_BINARY_NAME} PROPERTIES LINKER_LANGUAGE C C_STANDARD 99)
target_link_libraries(${TEST_BINARY_NAME} ${CMAKE_PROJECT_NAME})
target_compile_definitions(${TEST_BINARY_NAME} PRIVATE AWS_UNSTABLE_TESTING_API=1)
target_include_directories(${TEST_BINARY_NAME} PRIVATE ${CMAKE_CURRENT_LIST_DIR})

if (MSVC)
    target_compile_options(${TEST_BINARY_NAME} PRIVATE /W4 /WX)
else ()
    target_compile_options(${TEST_BINARY_NAME} PRIVATE -Wall -Wno-long-long -Werror)
endif ()

set(TEST_RUNNER_PATH ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/${TEST_BINARY_NAME})
add_test(test_incoming_no_op_valid ${TEST_RUNNER_PATH} test_incoming_no_op_valid)
add_test(test_incoming_application_data_no_headers_valid ${TEST_RUNNER_PATH} test_incoming_application_data_no_headers_valid)
add_test(test_incoming_application_one_compressed_header_pair_valid ${TEST_RUNNER_PATH} test_incoming_application_one_compressed_header_pair_valid)
add_test(test_incoming_application_int32_header_valid ${TEST_RUNNER_PATH} test_incoming_application_int32_header_valid)

add_test(test_outgoing_no_op_valid ${TEST_RUNNER_PATH} test_outgoing_no_op_valid)
add_test(test_outgoing_application_data_no_headers_valid ${TEST_RUNNER_PATH} test_outgoing_application_data_no_headers_valid)
add_test(test_outgoing_application_one_compressed_header_pair_valid ${TEST_RUNNER_PATH} test_outgoing_application_one_compressed_header_pair_valid)

add_test(test_streaming_decoder_incoming_no_op_valid_single_message ${TEST_RUNNER_PATH} test_streaming_decoder_incoming_no_op_valid_single_message)
add_test(test_streaming_decoder_incoming_application_no_headers ${TEST_RUNNER_PATH} test_streaming_decoder_incoming_application_no_headers)
add_test(test_streaming_decoder_incoming_application_one_compressed_header_pair_valid ${TEST_RUNNER_PATH} test_streaming_decoder_incoming_application_one_compressed_header_pair_valid)
add_test(test_streaming_decoder_incoming_multiple_messages ${TEST_RUNNER_PATH} test_streaming_decoder_incoming_multiple_messages)
