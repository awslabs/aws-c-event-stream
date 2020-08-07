/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/macros.h>
#include <aws/event-stream/event_stream_rpc_server.h>
#include <aws/io/channel_bootstrap.h>
#include <aws/io/socket.h>

#include <aws/testing/aws_test_harness.h>
#include <aws/testing/io_testing_channel.h>

struct test_data {
    struct aws_allocator *allocator;
    struct testing_channel testing_channel;
    struct aws_event_loop_group el_group;
    struct aws_server_bootstrap *bootstrap;
    struct aws_event_stream_rpc_server_listener *listener;
    struct aws_event_stream_rpc_connection *connection;
    aws_event_stream_rpc_server_connection_protocol_message_fn *received_fn;
    aws_event_stream_rpc_server_on_incoming_stream_fn *on_new_stream;
    aws_event_stream_rpc_server_stream_continuation_fn *on_continuation;
    aws_event_stream_rpc_server_stream_continuation_closed_fn *on_continuation_closed;
    void *user_data;
    void *continuation_user_data;
};

static struct test_data s_test_data;

static void s_fixture_on_protocol_message(
    struct aws_event_stream_rpc_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    struct test_data *test_data = user_data;
    test_data->received_fn(connection, message_args, test_data->user_data);
}

static void s_on_stream_continuation_shim(
    struct aws_event_stream_rpc_server_continuation_token *token,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    struct test_data *test_data = user_data;
    test_data->on_continuation(token, message_args, test_data->continuation_user_data);
}

static void s_stream_continuation_closed_shim(
    struct aws_event_stream_rpc_server_continuation_token *token,
    void *user_data) {
    struct test_data *test_data = user_data;
    test_data->on_continuation_closed(token, test_data->continuation_user_data);
}

static void s_on_incoming_stream_shim(
    struct aws_event_stream_rpc_server_continuation_token *token,
    struct aws_byte_cursor operation_name,
    struct aws_event_stream_rpc_server_stream_continuation_options *continuation_options,
    void *user_data) {
    struct test_data *test_data = user_data;

    continuation_options->on_continuation = s_on_stream_continuation_shim;
    continuation_options->on_continuation_closed = s_stream_continuation_closed_shim;
    continuation_options->user_data = test_data;

    if (test_data->on_new_stream) {
        test_data->on_new_stream(token, operation_name, continuation_options, test_data->continuation_user_data);
    }
}

static int s_fixture_on_new_connection(
    struct aws_event_stream_rpc_connection *connection,
    int error_code,
    struct aws_event_stream_rpc_connection_options *connection_options,
    void *user_data) {
    (void)connection;
    (void)error_code;
    (void)connection_options;
    (void)user_data;

    return AWS_OP_SUCCESS;
}

static void s_fixture_on_connection_shutdown(
    struct aws_event_stream_rpc_connection *connection,
    int error_code,
    void *user_data) {
    (void)connection;
    (void)error_code;
    (void)user_data;
}

static int s_fixture_setup(struct aws_allocator *allocator, void *ctx) {
    aws_event_stream_library_init(allocator);
    struct test_data *test_data = ctx;
    AWS_ZERO_STRUCT(*test_data);

    ASSERT_SUCCESS(aws_event_loop_group_default_init(&test_data->el_group, allocator, 0));
    test_data->bootstrap = aws_server_bootstrap_new(allocator, &test_data->el_group);
    ASSERT_NOT_NULL(test_data->bootstrap);

    struct aws_socket_options socket_options = {
        .connect_timeout_ms = 3000,
        .domain = AWS_SOCKET_IPV4,
        .type = AWS_SOCKET_STREAM,
    };

    struct aws_event_stream_rpc_server_listener_options listener_options = {
        .socket_options = &socket_options,
        .host_name = "127.0.0.1",
        .port = 30123,
        .bootstrap = test_data->bootstrap,
        .user_data = test_data,
        .on_new_connection = s_fixture_on_new_connection,
        .on_connection_shutdown = s_fixture_on_connection_shutdown,
    };

    test_data->listener = aws_event_stream_rpc_server_new_listener(allocator, &listener_options);
    ASSERT_NOT_NULL(test_data->listener);

    test_data->allocator = allocator;

    struct aws_testing_channel_options testing_channel_options = {
        .clock_fn = aws_high_res_clock_get_ticks,
    };
    ASSERT_SUCCESS(testing_channel_init(&test_data->testing_channel, allocator, &testing_channel_options));

    struct aws_event_stream_rpc_connection_options connection_options = {
        .on_connection_protocol_message = s_fixture_on_protocol_message,
        .on_incoming_stream = s_on_incoming_stream_shim,
        .user_data = test_data,
    };

    test_data->connection = aws_event_stream_rpc_server_connection_from_existing_channel(
        test_data->listener, test_data->testing_channel.channel, &connection_options);
    ASSERT_NOT_NULL(test_data->connection);

    testing_channel_run_currently_queued_tasks(&test_data->testing_channel);

    return AWS_OP_SUCCESS;
}

static int s_fixture_shutdown(struct aws_allocator *allocator, int setup_result, void *ctx) {
    (void)allocator;
    struct test_data *test_data = ctx;

    if (!setup_result) {
        aws_event_stream_rpc_server_connection_release(test_data->connection);
        testing_channel_clean_up(&test_data->testing_channel);
        aws_event_stream_rpc_server_listener_release(test_data->listener);
        aws_server_bootstrap_release(test_data->bootstrap);
        aws_event_loop_group_clean_up(&test_data->el_group);
    }

    aws_event_stream_library_clean_up();

    return AWS_OP_SUCCESS;
}

static int s_test_event_stream_rpc_server_connection_setup_and_teardown(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;
    (void)test_data;
    (void)allocator;
    /* just let setup and shutdown run to make sure the basic init/cleanup flow references are properly counted without
     * having to worry about continuation reference counts. */
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_setup_and_teardown,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_setup_and_teardown,
    s_fixture_shutdown,
    &s_test_data)

struct recieved_protocol_message_data {
    struct aws_allocator *allocator;
    enum aws_event_stream_rpc_message_type message_type;
    int message_flags;
    struct aws_byte_buf payload_cpy;
    bool message_flushed;
    int message_flush_err_code;
    bool continuation_closed;
    struct aws_event_stream_rpc_server_continuation_token *continuation_token;
    struct aws_byte_buf last_seen_operation_name;
};

static void s_on_recieved_protocol_message(
    struct aws_event_stream_rpc_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    struct recieved_protocol_message_data *message_data = user_data;
    message_data->message_type = message_args->message_type;
    message_data->message_flags = message_args->message_flags;
    aws_byte_buf_init_copy(&message_data->payload_cpy, message_data->allocator, message_args->payload);
}

static void s_on_message_flush_fn(int error_code, void *user_data) {
    struct recieved_protocol_message_data *message_data = user_data;

    message_data->message_flushed = true;
    message_data->message_flush_err_code = AWS_ERROR_SUCCESS;
}

static int s_test_event_stream_rpc_server_connection_connect_flow(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");

    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT, message_data.message_type);
    ASSERT_INT_EQUALS(0, message_data.message_flags);
    ASSERT_BIN_ARRAYS_EQUALS(
        payload.buffer, payload.len, message_data.payload_cpy.buffer, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);
    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_byte_buf_clean_up(&message_data.payload_cpy);

    struct aws_event_stream_rpc_message_args connect_ack_args = {
        .message_flags = AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_ACCEPTED,
        .message_type = AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK,
        .payload = &payload,
    };

    ASSERT_SUCCESS(aws_event_stream_rpc_server_connection_send_protocol_message(
        test_data->connection, &connect_ack_args, s_on_message_flush_fn, &message_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_TRUE(message_data.message_flushed);
    ASSERT_INT_EQUALS(0, message_data.message_flush_err_code);

    ASSERT_FALSE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_connect_flow,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_connect_flow,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_connect_reject_flow(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");

    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT, message_data.message_type);
    ASSERT_INT_EQUALS(0, message_data.message_flags);
    ASSERT_BIN_ARRAYS_EQUALS(
        payload.buffer, payload.len, message_data.payload_cpy.buffer, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);
    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_byte_buf_clean_up(&message_data.payload_cpy);

    struct aws_event_stream_rpc_message_args connect_ack_args = {
        .message_flags = AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_REJECTED,
        .message_type = AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK,
        .payload = &payload,
    };

    ASSERT_SUCCESS(aws_event_stream_rpc_server_connection_send_protocol_message(
        test_data->connection, &connect_ack_args, s_on_message_flush_fn, &message_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_TRUE(message_data.message_flushed);
    ASSERT_INT_EQUALS(0, message_data.message_flush_err_code);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_connect_reject_flow,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_connect_reject_flow,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_messages_before_connect_received(
    struct aws_allocator *allocator,
    void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");

    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        1));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    /* message should have just been outright rejected */
    ASSERT_INT_EQUALS(0, message_data.message_type);
    ASSERT_UINT_EQUALS(0, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));

    struct aws_linked_list *message_queue = testing_channel_get_written_message_queue(&test_data->testing_channel);
    ASSERT_FALSE(aws_linked_list_empty(message_queue));

    struct aws_linked_list_node *written_message_node = aws_linked_list_front(message_queue);
    struct aws_io_message *io_message = AWS_CONTAINER_OF(written_message_node, struct aws_io_message, queueing_handle);

    struct aws_event_stream_message written_message;
    ASSERT_SUCCESS(aws_event_stream_message_from_buffer(&written_message, allocator, &io_message->message_data));
    aws_array_list_clear(&headers_list);

    ASSERT_SUCCESS(aws_event_stream_message_headers(&written_message, &headers_list));

    enum aws_event_stream_rpc_message_type message_type = -1;

    for (size_t i = 0; aws_array_list_length(&headers_list); ++i) {
        struct aws_event_stream_header_value_pair *header = NULL;
        aws_array_list_get_at_ptr(&headers_list, (void **)&header, i);

        struct aws_byte_cursor header_name = aws_byte_cursor_from_array(header->header_name, header->header_name_len);

        if (aws_byte_cursor_eq(&aws_event_stream_rpc_message_type_name, &header_name)) {
            message_type = aws_event_stream_header_value_as_int32(header);
            break;
        }
    }

    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_event_stream_message_clean_up(&written_message);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, message_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_messages_before_connect_received,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_messages_before_connect_received,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_messages_before_connect_ack_sent(
    struct aws_allocator *allocator,
    void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");
    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT, message_data.message_type);
    ASSERT_INT_EQUALS(0, message_data.message_flags);
    ASSERT_BIN_ARRAYS_EQUALS(
        payload.buffer, payload.len, message_data.payload_cpy.buffer, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);
    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_byte_buf_clean_up(&message_data.payload_cpy);
    AWS_ZERO_STRUCT(message_data);

    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        1));

    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    aws_byte_buf_clean_up(&message_data.payload_cpy);
    ASSERT_INT_EQUALS(0, message_data.message_type);
    ASSERT_UINT_EQUALS(0, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));

    struct aws_linked_list *message_queue = testing_channel_get_written_message_queue(&test_data->testing_channel);
    ASSERT_FALSE(aws_linked_list_empty(message_queue));

    struct aws_linked_list_node *written_message_node = aws_linked_list_front(message_queue);
    struct aws_io_message *io_message = AWS_CONTAINER_OF(written_message_node, struct aws_io_message, queueing_handle);

    struct aws_event_stream_message written_message;
    ASSERT_SUCCESS(aws_event_stream_message_from_buffer(&written_message, allocator, &io_message->message_data));
    aws_array_list_clear(&headers_list);

    ASSERT_SUCCESS(aws_event_stream_message_headers(&written_message, &headers_list));

    enum aws_event_stream_rpc_message_type message_type = -1;

    for (size_t i = 0; aws_array_list_length(&headers_list); ++i) {
        struct aws_event_stream_header_value_pair *header = NULL;
        aws_array_list_get_at_ptr(&headers_list, (void **)&header, i);

        struct aws_byte_cursor header_name = aws_byte_cursor_from_array(header->header_name, header->header_name_len);

        if (aws_byte_cursor_eq(&aws_event_stream_rpc_message_type_name, &header_name)) {
            message_type = aws_event_stream_header_value_as_int32(header);
            break;
        }
    }

    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_event_stream_message_clean_up(&written_message);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, message_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_messages_before_connect_ack_sent,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_messages_before_connect_ack_sent,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_unknown_message_type(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");
    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        200));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(0, message_data.message_type);
    ASSERT_UINT_EQUALS(0, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));

    struct aws_linked_list *message_queue = testing_channel_get_written_message_queue(&test_data->testing_channel);
    ASSERT_FALSE(aws_linked_list_empty(message_queue));

    struct aws_linked_list_node *written_message_node = aws_linked_list_front(message_queue);
    struct aws_io_message *io_message = AWS_CONTAINER_OF(written_message_node, struct aws_io_message, queueing_handle);

    struct aws_event_stream_message written_message;
    ASSERT_SUCCESS(aws_event_stream_message_from_buffer(&written_message, allocator, &io_message->message_data));
    aws_array_list_clear(&headers_list);

    ASSERT_SUCCESS(aws_event_stream_message_headers(&written_message, &headers_list));

    enum aws_event_stream_rpc_message_type message_type = -1;

    for (size_t i = 0; aws_array_list_length(&headers_list); ++i) {
        struct aws_event_stream_header_value_pair *header = NULL;
        aws_array_list_get_at_ptr(&headers_list, (void **)&header, i);

        struct aws_byte_cursor header_name = aws_byte_cursor_from_array(header->header_name, header->header_name_len);

        if (aws_byte_cursor_eq(&aws_event_stream_rpc_message_type_name, &header_name)) {
            message_type = aws_event_stream_header_value_as_int32(header);
            break;
        }
    }

    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_event_stream_message_clean_up(&written_message);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, message_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_unknown_message_type,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_unknown_message_type,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_missing_message_type(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");
    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(0, message_data.message_type);
    ASSERT_UINT_EQUALS(0, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));

    struct aws_linked_list *message_queue = testing_channel_get_written_message_queue(&test_data->testing_channel);
    ASSERT_FALSE(aws_linked_list_empty(message_queue));

    struct aws_linked_list_node *written_message_node = aws_linked_list_front(message_queue);
    struct aws_io_message *io_message = AWS_CONTAINER_OF(written_message_node, struct aws_io_message, queueing_handle);

    struct aws_event_stream_message written_message;
    ASSERT_SUCCESS(aws_event_stream_message_from_buffer(&written_message, allocator, &io_message->message_data));
    aws_array_list_clear(&headers_list);

    ASSERT_SUCCESS(aws_event_stream_message_headers(&written_message, &headers_list));

    enum aws_event_stream_rpc_message_type message_type = -1;

    for (size_t i = 0; aws_array_list_length(&headers_list); ++i) {
        struct aws_event_stream_header_value_pair *header = NULL;
        aws_array_list_get_at_ptr(&headers_list, (void **)&header, i);

        struct aws_byte_cursor header_name = aws_byte_cursor_from_array(header->header_name, header->header_name_len);

        if (aws_byte_cursor_eq(&aws_event_stream_rpc_message_type_name, &header_name)) {
            message_type = aws_event_stream_header_value_as_int32(header);
            break;
        }
    }

    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_event_stream_message_clean_up(&written_message);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, message_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_missing_message_type,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_missing_message_type,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_missing_message_flags(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");
    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(0, message_data.message_type);
    ASSERT_UINT_EQUALS(0, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));

    struct aws_linked_list *message_queue = testing_channel_get_written_message_queue(&test_data->testing_channel);
    ASSERT_FALSE(aws_linked_list_empty(message_queue));

    struct aws_linked_list_node *written_message_node = aws_linked_list_front(message_queue);
    struct aws_io_message *io_message = AWS_CONTAINER_OF(written_message_node, struct aws_io_message, queueing_handle);

    struct aws_event_stream_message written_message;
    ASSERT_SUCCESS(aws_event_stream_message_from_buffer(&written_message, allocator, &io_message->message_data));
    aws_array_list_clear(&headers_list);

    ASSERT_SUCCESS(aws_event_stream_message_headers(&written_message, &headers_list));

    enum aws_event_stream_rpc_message_type message_type = -1;

    for (size_t i = 0; aws_array_list_length(&headers_list); ++i) {
        struct aws_event_stream_header_value_pair *header = NULL;
        aws_array_list_get_at_ptr(&headers_list, (void **)&header, i);

        struct aws_byte_cursor header_name = aws_byte_cursor_from_array(header->header_name, header->header_name_len);

        if (aws_byte_cursor_eq(&aws_event_stream_rpc_message_type_name, &header_name)) {
            message_type = aws_event_stream_header_value_as_int32(header);
            break;
        }
    }

    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_event_stream_message_clean_up(&written_message);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, message_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_missing_message_flags,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_missing_message_flags,
    s_fixture_shutdown,
    &s_test_data)

static int s_test_event_stream_rpc_server_connection_missing_stream_id(struct aws_allocator *allocator, void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");
    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(0, message_data.message_type);
    ASSERT_UINT_EQUALS(0, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);

    ASSERT_TRUE(aws_event_stream_rpc_server_connection_is_closed(test_data->connection));

    struct aws_linked_list *message_queue = testing_channel_get_written_message_queue(&test_data->testing_channel);
    ASSERT_FALSE(aws_linked_list_empty(message_queue));

    struct aws_linked_list_node *written_message_node = aws_linked_list_front(message_queue);
    struct aws_io_message *io_message = AWS_CONTAINER_OF(written_message_node, struct aws_io_message, queueing_handle);

    struct aws_event_stream_message written_message;
    ASSERT_SUCCESS(aws_event_stream_message_from_buffer(&written_message, allocator, &io_message->message_data));
    aws_array_list_clear(&headers_list);

    ASSERT_SUCCESS(aws_event_stream_message_headers(&written_message, &headers_list));

    enum aws_event_stream_rpc_message_type message_type = -1;

    for (size_t i = 0; aws_array_list_length(&headers_list); ++i) {
        struct aws_event_stream_header_value_pair *header = NULL;
        aws_array_list_get_at_ptr(&headers_list, (void **)&header, i);

        struct aws_byte_cursor header_name = aws_byte_cursor_from_array(header->header_name, header->header_name_len);

        if (aws_byte_cursor_eq(&aws_event_stream_rpc_message_type_name, &header_name)) {
            message_type = aws_event_stream_header_value_as_int32(header);
            break;
        }
    }

    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_event_stream_message_clean_up(&written_message);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, message_type);

    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_missing_stream_id,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_missing_stream_id,
    s_fixture_shutdown,
    &s_test_data)

static void s_on_incoming_stream(
    struct aws_event_stream_rpc_server_continuation_token *token,
    struct aws_byte_cursor operation_name,
    struct aws_event_stream_rpc_server_stream_continuation_options *continuation_options,
    void *user_data) {

    struct recieved_protocol_message_data *message_data = user_data;
    message_data->continuation_token = token;
    aws_event_stream_rpc_server_continuation_acquire(token);
    aws_byte_buf_init_copy_from_cursor(
        &message_data->last_seen_operation_name, message_data->allocator, operation_name);
}

static void s_on_continuation_message(
    struct aws_event_stream_rpc_server_continuation_token *token,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    struct recieved_protocol_message_data *message_data = user_data;
    message_data->message_type = message_args->message_type;
    message_data->message_flags = message_args->message_flags;
    aws_byte_buf_init_copy(&message_data->payload_cpy, message_data->allocator, message_args->payload);
}

static void s_on_continuation_closed(struct aws_event_stream_rpc_server_continuation_token *token, void *user_data) {
    struct recieved_protocol_message_data *message_data = user_data;
    message_data->continuation_closed = true;
}

static int s_test_event_stream_rpc_server_connection_continuation_messages_flow(
    struct aws_allocator *allocator,
    void *ctx) {
    struct test_data *test_data = ctx;

    struct recieved_protocol_message_data message_data = {
        .allocator = allocator,
    };

    test_data->user_data = &message_data;
    test_data->received_fn = s_on_recieved_protocol_message;
    test_data->continuation_user_data = &message_data;
    test_data->on_continuation = s_on_continuation_message;
    test_data->on_continuation_closed = s_on_continuation_closed;
    test_data->on_new_stream = s_on_incoming_stream;

    struct aws_byte_buf payload = aws_byte_buf_from_c_str("test connect message payload");

    struct aws_array_list headers_list;
    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        0));

    struct aws_event_stream_message message;
    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    struct aws_byte_cursor send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT, message_data.message_type);
    ASSERT_INT_EQUALS(0, message_data.message_flags);
    ASSERT_BIN_ARRAYS_EQUALS(
        payload.buffer, payload.len, message_data.payload_cpy.buffer, message_data.payload_cpy.len);

    aws_event_stream_message_clean_up(&message);
    aws_event_stream_headers_list_cleanup(&headers_list);
    aws_byte_buf_clean_up(&message_data.payload_cpy);

    struct aws_event_stream_rpc_message_args connect_ack_args = {
        .message_flags = AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_ACCEPTED,
        .message_type = AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK,
        .payload = &payload,
    };

    ASSERT_SUCCESS(aws_event_stream_rpc_server_connection_send_protocol_message(
        test_data->connection, &connect_ack_args, s_on_message_flush_fn, &message_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);

    ASSERT_TRUE(message_data.message_flushed);
    ASSERT_INT_EQUALS(0, message_data.message_flush_err_code);

    ASSERT_SUCCESS(aws_event_stream_headers_list_init(&headers_list, allocator));

    struct aws_byte_cursor operation_name = aws_byte_cursor_from_c_str("testOperation");
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        aws_event_stream_rpc_message_type_name.len,
        AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        aws_event_stream_rpc_message_flags_name.len,
        0));
    ASSERT_SUCCESS(aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        aws_event_stream_rpc_stream_id_name.len,
        1));
    ASSERT_SUCCESS(aws_event_stream_add_string_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_operation_name.ptr,
        aws_event_stream_rpc_operation_name.len,
        (const char *)operation_name.ptr,
        operation_name.len,
        0));

    ASSERT_SUCCESS(aws_event_stream_message_init(&message, allocator, &headers_list, &payload));

    send_data = aws_byte_cursor_from_array(
        aws_event_stream_message_buffer(&message), aws_event_stream_message_total_length(&message));
    ASSERT_SUCCESS(testing_channel_push_read_data(&test_data->testing_channel, send_data));
    testing_channel_drain_queued_tasks(&test_data->testing_channel);
    aws_event_stream_message_clean_up(&message);
    aws_event_stream_headers_list_cleanup(&headers_list);

    ASSERT_NOT_NULL(message_data.continuation_token);
    ASSERT_INT_EQUALS(AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE, message_data.message_type);
    ASSERT_BIN_ARRAYS_EQUALS(
        operation_name.ptr,
        operation_name.len,
        message_data.last_seen_operation_name.buffer,
        message_data.last_seen_operation_name.len);
    ASSERT_BIN_ARRAYS_EQUALS(
        payload.buffer, payload.len, message_data.payload_cpy.buffer, message_data.payload_cpy.len);

    aws_byte_buf_clean_up(&message_data.last_seen_operation_name);
    aws_byte_buf_clean_up(&message_data.payload_cpy);

    /* TODO, send one from the server, and one more from the client with the terminate stream flag. */

    aws_event_stream_rpc_server_connection_close(test_data->connection, AWS_ERROR_SUCCESS);
    testing_channel_drain_queued_tasks(&test_data->testing_channel);
    aws_event_stream_rpc_server_continuation_release(message_data.continuation_token);

    ASSERT_TRUE(message_data.continuation_closed);
    return AWS_OP_SUCCESS;
}

AWS_TEST_CASE_FIXTURE(
    test_event_stream_rpc_server_connection_continuation_messages_flow,
    s_fixture_setup,
    s_test_event_stream_rpc_server_connection_continuation_messages_flow,
    s_fixture_shutdown,
    &s_test_data)
