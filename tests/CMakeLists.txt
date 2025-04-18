# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0.
include(AwsTestHarness)
enable_testing()

file(GLOB TEST_SRC "*.c")
file(GLOB TESTS ${TEST_SRC})

set(TEST_BINARY_NAME ${PROJECT_NAME}-tests)

add_test_case(test_incoming_no_op_valid)
add_test_case(test_incoming_application_data_no_headers_valid)
add_test_case(test_incoming_application_one_compressed_header_pair_valid)
add_test_case(test_incoming_application_int32_header_valid)

add_test_case(test_outgoing_no_op_valid)
add_test_case(test_outgoing_application_data_no_headers_valid)
add_test_case(test_outgoing_application_one_compressed_header_pair_valid)

add_test_case(test_streaming_decoder_incoming_no_op_valid_single_message)
add_test_case(test_streaming_decoder_incoming_application_no_headers)
add_test_case(test_streaming_decoder_incoming_application_one_compressed_header_pair_valid)
add_test_case(test_streaming_decoder_incoming_application_one_int32_header_pair_valid)
add_test_case(test_streaming_decoder_incoming_application_variable_headers_with_empty_length_pair_valid)
add_test_case(test_streaming_decoder_incoming_application_one_bool_header_pair_valid)
add_test_case(test_streaming_decoder_incoming_multiple_messages)

add_test_case(test_channel_handler_single_valid_messages_parse)
add_test_case(test_channel_handler_multiple_valid_messages_parse)
add_test_case(test_channel_handler_corrupted_crc_fails)
add_test_case(test_channel_handler_large_msg_success)
add_test_case(test_channel_handler_write_message)

add_net_test_case(test_event_stream_rpc_server_connection_setup_and_teardown)
add_net_test_case(test_event_stream_rpc_server_connection_setup_and_teardown_with_bind_to_zero_port)
add_net_test_case(test_event_stream_rpc_server_connection_connect_flow)
add_net_test_case(test_event_stream_rpc_server_connection_connect_reject_flow)
add_net_test_case(test_event_stream_rpc_server_connection_messages_before_connect_received)
add_net_test_case(test_event_stream_rpc_server_connection_messages_before_connect_ack_sent)
add_net_test_case(test_event_stream_rpc_server_connection_unknown_message_type)
add_net_test_case(test_event_stream_rpc_server_connection_missing_message_type)
add_net_test_case(test_event_stream_rpc_server_connection_missing_message_flags)
add_net_test_case(test_event_stream_rpc_server_connection_continuation_missing_operation)
add_net_test_case(test_event_stream_rpc_server_connection_missing_stream_id)
add_net_test_case(test_event_stream_rpc_server_connection_continuation_messages_flow)
add_net_test_case(test_event_stream_rpc_server_connection_continuation_failure)
add_net_test_case(test_event_stream_rpc_server_connection_stream_id_ahead)
add_net_test_case(test_event_stream_rpc_server_connection_continuation_reused_stream_id_fails)
add_net_test_case(test_event_stream_rpc_server_connection_continuation_max_stream_id_reached)

add_net_test_case(test_event_stream_rpc_client_connection_setup_and_teardown)
add_net_test_case(test_event_stream_rpc_client_connection_connect)
add_net_test_case(test_event_stream_rpc_client_connection_message_before_connect)
add_net_test_case(test_event_stream_rpc_client_connection_protocol_message)
add_net_test_case(test_event_stream_rpc_client_connection_continuation_flow)
add_net_test_case(test_event_stream_rpc_client_connection_unactivated_continuation_fails)
add_net_test_case(test_event_stream_rpc_client_connection_continuation_send_message_on_closed_fails)
add_net_test_case(test_event_stream_rpc_client_connection_continuation_duplicated_activate_fails)

generate_test_driver(${TEST_BINARY_NAME})
