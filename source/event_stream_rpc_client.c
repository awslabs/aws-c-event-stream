/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/event-stream/event_stream_channel_handler.h>
#include <aws/event-stream/event_stream_rpc_client.h>
#include <aws/event-stream/private/event_stream_rpc_priv.h>

#include <aws/common/atomics.h>
#include <aws/common/hash_table.h>
#include <aws/common/mutex.h>

#include <aws/io/channel_bootstrap.h>

#ifdef _MSC_VER
/* allow declared initializer using address of automatic variable */
#    pragma warning(disable : 4221)
#endif

struct aws_event_stream_rpc_client_connection {
    struct aws_allocator *allocator;
    struct aws_hash_table continuation_table;
    struct aws_client_bootstrap *bootstrap_ref;
    struct aws_atomic_var ref_count;
    struct aws_channel *channel;
    struct aws_channel_handler *event_stream_handler;
    uint32_t latest_stream_id;
    struct aws_mutex stream_lock;
    struct aws_atomic_var is_closed;
    struct aws_atomic_var handshake_complete;
    size_t initial_window_size;
    aws_event_stream_rpc_client_on_connection_setup_fn *on_connection_setup;
    aws_event_stream_rpc_client_connection_protocol_message_fn *on_connection_protocol_message;
    aws_event_stream_rpc_client_on_connection_shutdown_fn *on_connection_shutdown;
    void *user_data;
    bool bootstrap_owned;
    bool enable_read_back_pressure;
};

struct aws_event_stream_rpc_client_continuation_token {
    uint32_t stream_id;
    struct aws_event_stream_rpc_client_connection *connection;
    aws_event_stream_rpc_client_stream_continuation_fn *continuation_fn;
    aws_event_stream_rpc_client_stream_continuation_closed_fn *closed_fn;
    void *user_data;
    struct aws_atomic_var ref_count;
    struct aws_atomic_var is_closed;
};

static void s_on_message_received(struct aws_event_stream_message *message, int error_code, void *user_data);

static int s_create_connection_on_channel(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_channel *channel) {
    struct aws_channel_handler *event_stream_handler = NULL;
    struct aws_channel_slot *slot = NULL;

    struct aws_event_stream_channel_handler_options handler_options = {
        .on_message_received = s_on_message_received,
        .user_data = connection,
        .initial_window_size = connection->initial_window_size,
        .manual_window_management = connection->enable_read_back_pressure,
    };

    event_stream_handler = aws_event_stream_channel_handler_new(connection->allocator, &handler_options);

    if (!event_stream_handler) {
        goto error;
    }

    slot = aws_channel_slot_new(channel);

    if (!slot) {
        goto error;
    }

    aws_channel_slot_insert_end(channel, slot);
    if (aws_channel_slot_set_handler(slot, event_stream_handler)) {
        goto error;
    }

    connection->event_stream_handler = event_stream_handler;
    connection->channel = channel;
    aws_channel_acquire_hold(channel);

    return AWS_OP_SUCCESS;

error:
    if (!slot && event_stream_handler) {
        aws_channel_handler_destroy(event_stream_handler);
    }

    return AWS_OP_ERR;
}

static void s_on_channel_setup_fn(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;

    struct aws_event_stream_rpc_client_connection *connection = user_data;

    if (!error_code) {
        connection->bootstrap_owned = true;
        if (s_create_connection_on_channel(connection, channel)) {
            int last_error = aws_last_error();
            connection->on_connection_setup(NULL, last_error, connection->user_data);
            aws_channel_shutdown(channel, last_error);
            return;
        }

        aws_event_stream_rpc_client_connection_acquire(connection);
        connection->on_connection_setup(connection, AWS_OP_SUCCESS, connection->user_data);
        aws_event_stream_rpc_client_connection_release(connection);
    } else {
        connection->on_connection_setup(NULL, error_code, connection->user_data);
        aws_event_stream_rpc_client_connection_release(connection);
    }
}

static void s_on_channel_shutdown_fn(
    struct aws_client_bootstrap *bootstrap,
    int error_code,
    struct aws_channel *channel,
    void *user_data) {
    (void)bootstrap;

    struct aws_event_stream_rpc_client_connection *connection = user_data;
    aws_atomic_store_int(&connection->is_closed, 1u);

    if (connection->bootstrap_owned) {
        aws_mutex_lock(&connection->stream_lock);
        aws_hash_table_clear(&connection->continuation_table);
        aws_mutex_unlock(&connection->stream_lock);
        aws_event_stream_rpc_client_connection_acquire(connection);
        connection->on_connection_shutdown(connection, error_code, connection->user_data);
        aws_event_stream_rpc_client_connection_release(connection);
    }

    aws_channel_release_hold(channel);
    aws_event_stream_rpc_client_connection_release(connection);
}

static void s_continuation_destroy(void *value) {
    struct aws_event_stream_rpc_client_continuation_token *token = value;

    if (token->stream_id) {
        token->closed_fn(token, token->user_data);
    }

    aws_event_stream_rpc_client_continuation_release(token);
}

int aws_event_stream_rpc_client_connection_connect(
    struct aws_allocator *allocator,
    const struct aws_event_stream_rpc_client_connection_options *conn_options) {
    AWS_PRECONDITION(allocator);
    AWS_PRECONDITION(conn_options);
    AWS_PRECONDITION(conn_options->on_connection_protocol_message);
    AWS_PRECONDITION(conn_options->on_connection_setup);
    AWS_PRECONDITION(conn_options->on_connection_shutdown);

    struct aws_event_stream_rpc_client_connection *connection =
        aws_mem_calloc(allocator, 1, sizeof(struct aws_event_stream_rpc_client_connection));

    if (!connection) {
        return AWS_OP_ERR;
    }

    connection->allocator = allocator;
    aws_atomic_init_int(&connection->ref_count, 1);
    connection->bootstrap_ref = conn_options->bootstrap;
    /* this is released in the connection release which gets called regardless of if this function is successful or
     * not*/
    aws_client_bootstrap_acquire(connection->bootstrap_ref);
    aws_atomic_init_int(&connection->handshake_complete, 0);
    aws_atomic_init_int(&connection->is_closed, 0);
    aws_mutex_init(&connection->stream_lock);

    connection->on_connection_shutdown = conn_options->on_connection_shutdown;
    connection->on_connection_protocol_message = conn_options->on_connection_protocol_message;
    connection->on_connection_setup = conn_options->on_connection_setup;
    connection->user_data = conn_options->user_data;

    if (aws_hash_table_init(
            &connection->continuation_table,
            allocator,
            64,
            aws_event_stream_rpc_hash_streamid,
            aws_event_stream_rpc_streamid_eq,
            NULL,
            s_continuation_destroy)) {
        goto error;
    }

    struct aws_socket_channel_bootstrap_options bootstrap_options = {
        .bootstrap = connection->bootstrap_ref,
        .tls_options = conn_options->tls_options,
        .socket_options = conn_options->socket_options,
        .user_data = connection,
        .host_name = conn_options->host_name,
        .port = conn_options->port,
        .enable_read_back_pressure = false,
        .setup_callback = s_on_channel_setup_fn,
        .shutdown_callback = s_on_channel_shutdown_fn,
    };

    if (aws_client_bootstrap_new_socket_channel(&bootstrap_options)) {
        goto error;
    }

    return AWS_OP_SUCCESS;

error:
    aws_event_stream_rpc_client_connection_release(connection);
    return AWS_OP_ERR;
}

void aws_event_stream_rpc_client_connection_acquire(const struct aws_event_stream_rpc_client_connection *connection) {
    AWS_PRECONDITION(connection);
    aws_atomic_fetch_add_explicit(
        &((struct aws_event_stream_rpc_client_connection *)connection)->ref_count, 1, aws_memory_order_relaxed);
}

static void s_destroy_connection(struct aws_event_stream_rpc_client_connection *connection) {
    aws_hash_table_clean_up(&connection->continuation_table);
    aws_client_bootstrap_release(connection->bootstrap_ref);
    aws_mem_release(connection->allocator, connection);
}

void aws_event_stream_rpc_client_connection_release(const struct aws_event_stream_rpc_client_connection *connection) {
    if (!connection) {
        return;
    }

    struct aws_event_stream_rpc_client_connection *connection_mut =
        (struct aws_event_stream_rpc_client_connection *)connection;
    size_t ref_count = aws_atomic_fetch_sub_explicit(&connection_mut->ref_count, 1, aws_memory_order_seq_cst);

    if (ref_count == 1) {
        s_destroy_connection(connection_mut);
    }
}

void aws_event_stream_rpc_client_connection_close(
    struct aws_event_stream_rpc_client_connection *connection,
    int shutdown_error_code) {

    if (!aws_event_stream_rpc_client_connection_is_closed(connection)) {
        aws_atomic_store_int(&connection->is_closed, 1U);
        aws_channel_shutdown(connection->channel, shutdown_error_code);

        if (!connection->bootstrap_owned) {
            aws_mutex_lock(&connection->stream_lock);
            aws_hash_table_clear(&connection->continuation_table);
            aws_mutex_unlock(&connection->stream_lock);
            aws_event_stream_rpc_client_connection_release(connection);
        }
    }
}

bool aws_event_stream_rpc_client_connection_is_closed(const struct aws_event_stream_rpc_client_connection *connection) {
    return aws_atomic_load_int(&connection->is_closed) == 1U;
}

struct event_stream_connection_send_message_args {
    struct aws_allocator *allocator;
    struct aws_event_stream_message message;
    enum aws_event_stream_rpc_message_type message_type;
    struct aws_event_stream_rpc_client_connection *connection;
    struct aws_event_stream_rpc_client_continuation_token *continuation;
    aws_event_stream_rpc_client_message_flush_fn *flush_fn;
    void *user_data;
    bool end_stream;
    bool terminate_connection;
};

static void s_on_protocol_message_written_fn(
    struct aws_event_stream_message *message,
    int error_code,
    void *user_data) {
    (void)message;

    struct event_stream_connection_send_message_args *message_args = user_data;

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT) {
        aws_atomic_store_int(&message_args->connection->handshake_complete, 1);
    }

    if (message_args->end_stream) {
        AWS_FATAL_ASSERT(message_args->continuation && "end stream flag was set but it wasn't on a continuation");
        aws_atomic_store_int(&message_args->continuation->is_closed, 1U);
        aws_hash_table_remove(
            &message_args->connection->continuation_table, &message_args->continuation->stream_id, NULL, NULL);
    }

    message_args->flush_fn(error_code, message_args->user_data);

    if (message_args->terminate_connection) {
        aws_event_stream_rpc_client_connection_close(message_args->connection, AWS_ERROR_SUCCESS);
    }

    aws_event_stream_rpc_client_connection_release(message_args->connection);

    if (message_args->continuation) {
        aws_event_stream_rpc_client_continuation_release(message_args->continuation);
    }

    aws_event_stream_message_clean_up(&message_args->message);
    aws_mem_release(message_args->allocator, message_args);
}

static int s_send_protocol_message(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    struct aws_byte_cursor *operation_name,
    const struct aws_event_stream_rpc_message_args *message_args,
    int32_t stream_id,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    size_t connect_handshake_completion = aws_atomic_load_int(&connection->handshake_complete);

    /* handshake step 1 is a connect message being received. Handshake 2 is the connect ack being sent.
     * no messages other than connect and connect ack are allowed until this count reaches 2. */
    if (connect_handshake_completion != 2 && message_args->message_type < AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT) {
        return aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
    }

    struct event_stream_connection_send_message_args *args =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct event_stream_connection_send_message_args));

    if (!message_args) {
        return AWS_OP_ERR;
    }

    args->allocator = connection->allocator;
    args->user_data = user_data;
    args->message_type = message_args->message_type;
    args->connection = connection;
    args->flush_fn = flush_fn;

    if (continuation) {
        args->continuation = continuation;
        aws_event_stream_rpc_client_continuation_acquire(continuation);

        if (message_args->message_flags & AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_TERMINATE_STREAM) {
            args->end_stream = true;
        }
    }

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK &&
        !(message_args->message_flags & AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_ACCEPTED)) {
        args->terminate_connection = true;
    }

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT) {
        aws_atomic_store_int(&connection->handshake_complete, 1u);
    }

    args->flush_fn = flush_fn;

    size_t headers_count = operation_name ? message_args->headers_count + 4 : message_args->headers_count + 3;
    struct aws_array_list headers_list;
    AWS_ZERO_STRUCT(headers_list);

    if (aws_array_list_init_dynamic(
            &headers_list, connection->allocator, headers_count, sizeof(struct aws_event_stream_header_value_pair))) {
        goto args_allocated_before_failure;
    }

    /* since we preallocated the space for the headers, these can't fail, but we'll go ahead an assert on them just in
     * case */
    for (size_t i = 0; i < message_args->headers_count; ++i) {
        AWS_FATAL_ASSERT(!aws_array_list_push_back(&headers_list, &message_args->headers[i]));
    }

    AWS_FATAL_ASSERT(!aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_type_name.ptr,
        (uint8_t)aws_event_stream_rpc_message_type_name.len,
        message_args->message_type));
    AWS_FATAL_ASSERT(!aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_message_flags_name.ptr,
        (uint8_t)aws_event_stream_rpc_message_flags_name.len,
        message_args->message_flags));
    AWS_FATAL_ASSERT(!aws_event_stream_add_int32_header(
        &headers_list,
        (const char *)aws_event_stream_rpc_stream_id_name.ptr,
        (uint8_t)aws_event_stream_rpc_stream_id_name.len,
        stream_id));

    if (operation_name) {
        AWS_FATAL_ASSERT(!aws_event_stream_add_string_header(
            &headers_list,
            (const char *)aws_event_stream_rpc_operation_name.ptr,
            (uint8_t)aws_event_stream_rpc_operation_name.len,
            (const char *)operation_name->ptr,
            (uint16_t)operation_name->len,
            0));
    }

    int message_init_err_code =
        aws_event_stream_message_init(&args->message, connection->allocator, &headers_list, message_args->payload);
    aws_array_list_clean_up(&headers_list);

    if (message_init_err_code) {
        goto args_allocated_before_failure;
    }

    aws_event_stream_rpc_client_connection_acquire(connection);

    if (aws_event_stream_channel_handler_write_message(
            connection->event_stream_handler, &args->message, s_on_protocol_message_written_fn, args)) {
        goto message_initialized_before_failure;
    }

    return AWS_OP_SUCCESS;

message_initialized_before_failure:
    aws_event_stream_message_clean_up(&args->message);

args_allocated_before_failure:
    aws_mem_release(args->allocator, args);
    aws_event_stream_rpc_client_connection_release(connection);

    return AWS_OP_ERR;
}

int aws_event_stream_rpc_client_connection_send_protocol_message(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {
    if (aws_event_stream_rpc_client_connection_is_closed(connection)) {
        return aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_CONNECTION_CLOSED);
    }

    return s_send_protocol_message(connection, NULL, NULL, message_args, 0, flush_fn, user_data);
}

static void s_connection_error_message_flush_fn(int error_code, void *user_data) {
    (void)error_code;

    struct aws_event_stream_rpc_client_connection *connection = user_data;
    aws_event_stream_rpc_client_connection_close(connection, AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
}

static void s_send_connection_level_error(
    struct aws_event_stream_rpc_client_connection *connection,
    uint32_t message_type,
    uint32_t message_flags,
    const struct aws_byte_cursor *message) {
    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(message->ptr, message->len);

    struct aws_event_stream_header_value_pair content_type_header =
        aws_event_stream_create_string_header(s_json_content_type_name, s_json_content_type_value);

    struct aws_event_stream_header_value_pair headers[] = {
        content_type_header,
    };

    struct aws_event_stream_rpc_message_args message_args = {
        .message_type = message_type,
        .message_flags = message_flags,
        .payload = &payload_buf,
        .headers_count = 1,
        .headers = headers,
    };

    aws_event_stream_rpc_client_connection_send_protocol_message(
        connection, &message_args, s_connection_error_message_flush_fn, connection);
}

static void s_route_message_by_type(
    struct aws_event_stream_rpc_client_connection *connection,
    struct aws_event_stream_message *message,
    struct aws_array_list *headers_list,
    uint32_t stream_id,
    uint32_t message_type,
    uint32_t message_flags) {
    struct aws_byte_buf payload_buf = aws_byte_buf_from_array(
        aws_event_stream_message_payload(message), aws_event_stream_message_payload_len(message));

    struct aws_event_stream_rpc_message_args message_args = {
        .headers = headers_list->data,
        .headers_count = aws_array_list_length(headers_list),
        .payload = &payload_buf,
        .message_flags = message_flags,
        .message_type = message_type,
    };

    size_t handshake_complete = aws_atomic_load_int(&connection->handshake_complete);

    /* make sure if this is not a CONNECT message being received, the handshake has been completed. */
    if (handshake_complete < 2 && message_type != AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK) {
        aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
        s_send_connection_level_error(
            connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_connect_not_completed_error);
        return;
    }

    /* stream_id being non zero ALWAYS indicates APPLICATION_DATA or APPLICATION_ERROR. */
    if (stream_id > 0) {
        struct aws_event_stream_rpc_client_continuation_token *continuation = NULL;
        if (message_type > AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_ERROR) {
            aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_stream_id_error);
            return;
        }

        aws_mutex_lock(&connection->stream_lock);
        struct aws_hash_element *continuation_element = NULL;
        if (aws_hash_table_find(&connection->continuation_table, &stream_id, &continuation_element) ||
            !continuation_element) {
            aws_mutex_unlock(&connection->stream_lock);
            aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_PROTOCOL_ERROR);
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_client_stream_id_error);
            return;
        }

        aws_mutex_unlock(&connection->stream_lock);

        continuation = continuation_element->value;
        aws_event_stream_rpc_client_continuation_acquire(continuation);
        continuation->continuation_fn(continuation, &message_args, continuation->user_data);
        aws_event_stream_rpc_client_continuation_release(continuation);

        /* if it was a terminal stream message purge it from the hash table. The delete will decref the continuation. */
        if (message_flags & AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_TERMINATE_STREAM) {
            aws_atomic_store_int(&continuation->is_closed, 1U);
            aws_mutex_lock(&connection->stream_lock);
            aws_hash_table_remove(&connection->continuation_table, &stream_id, NULL, NULL);
            aws_mutex_unlock(&connection->stream_lock);
        }
    } else {
        if (message_type <= AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_ERROR ||
            message_type >= AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_COUNT) {
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_message_type_error);
            return;
        }

        if (message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK) {
            if (handshake_complete != 1u) {
                /* only one connect is allowed. This would be a duplicate. */
                s_send_connection_level_error(
                    connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_connect_not_completed_error);
                return;
            }
            aws_atomic_store_int(&connection->handshake_complete, 2U);
        }

        connection->on_connection_protocol_message(connection, &message_args, connection->user_data);
    }
}

/* invoked by the event stream channel handler when a complete message has been read from the channel. */
static void s_on_message_received(struct aws_event_stream_message *message, int error_code, void *user_data) {

    if (!error_code) {
        struct aws_event_stream_rpc_client_connection *connection = user_data;

        struct aws_array_list headers;
        if (aws_array_list_init_dynamic(
                &headers, connection->allocator, 8, sizeof(struct aws_event_stream_header_value_pair))) {
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_INTERNAL_ERROR, 0, &s_internal_error);
            return;
        }

        if (aws_event_stream_message_headers(message, &headers)) {
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_INTERNAL_ERROR, 0, &s_internal_error);
            goto clean_up;
        }

        int32_t stream_id = -1;
        int32_t message_type = -1;
        int32_t message_flags = -1;

        struct aws_byte_buf operation_name_buf;
        AWS_ZERO_STRUCT(operation_name_buf);
        if (aws_event_stream_rpc_fetch_message_metadata(
                &headers, &stream_id, &message_type, &message_flags, &operation_name_buf)) {
            s_send_connection_level_error(
                connection, AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR, 0, &s_invalid_message_error);
            goto clean_up;
        }

        (void)operation_name_buf;

        s_route_message_by_type(connection, message, &headers, stream_id, message_type, message_flags);

    clean_up:
        aws_event_stream_headers_list_cleanup(&headers);
    }
}

struct aws_event_stream_rpc_client_continuation_token *aws_event_stream_rpc_client_connection_new_stream(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_client_stream_continuation_options *continuation_options) {
    AWS_PRECONDITION(continuation_options->on_continuation_closed);
    AWS_PRECONDITION(continuation_options->on_continuation);

    struct aws_event_stream_rpc_client_continuation_token *continuation =
        aws_mem_calloc(connection->allocator, 1, sizeof(struct aws_event_stream_rpc_client_continuation_token));

    if (!continuation) {
        return NULL;
    }

    continuation->connection = connection;
    aws_event_stream_rpc_client_connection_acquire(continuation->connection);
    aws_atomic_init_int(&continuation->ref_count, 1);
    aws_atomic_init_int(&continuation->is_closed, 0);
    continuation->continuation_fn = continuation_options->on_continuation;
    continuation->closed_fn = continuation_options->on_continuation_closed;
    continuation->user_data = continuation_options->user_data;

    return continuation;
}

void *aws_event_stream_rpc_client_continuation_get_user_data(
    struct aws_event_stream_rpc_client_continuation_token *continuation) {
    return continuation->user_data;
}

void aws_event_stream_rpc_client_continuation_acquire(
    const struct aws_event_stream_rpc_client_continuation_token *continuation) {
    aws_atomic_fetch_add_explicit(
        &((struct aws_event_stream_rpc_client_continuation_token *)continuation)->ref_count,
        1u,
        aws_memory_order_relaxed);
}

void aws_event_stream_rpc_client_continuation_release(
    const struct aws_event_stream_rpc_client_continuation_token *continuation) {
    if (!continuation) {
        return;
    }

    struct aws_event_stream_rpc_client_continuation_token *continuation_mut =
        (struct aws_event_stream_rpc_client_continuation_token *)continuation;
    size_t ref_count = aws_atomic_fetch_sub_explicit(&continuation_mut->ref_count, 1, aws_memory_order_seq_cst);

    if (ref_count == 1) {
        struct aws_allocator *allocator = continuation_mut->connection->allocator;
        aws_event_stream_rpc_client_connection_release(continuation_mut->connection);
        aws_mem_release(allocator, continuation_mut);
    }
}

bool aws_event_stream_rpc_client_continuation_is_closed(
    const struct aws_event_stream_rpc_client_continuation_token *continuation) {
    return aws_atomic_load_int(&continuation->is_closed) == 1u;
}

int aws_event_stream_rpc_client_continuation_activate(
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    struct aws_byte_cursor operation_name,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    int ret_val = AWS_OP_ERR;

    aws_mutex_lock(&continuation->connection->stream_lock);

    if (continuation->stream_id) {
        aws_raise_error(AWS_ERROR_INVALID_STATE);
        goto clean_up;
    }

    /* we cannot update the connection's stream id until we're certain the message at least made it to the wire, because
     * the next stream id must be consecutively increasing by 1. So send the message then update the connection state
     * once we've made it to the wire. */
    continuation->stream_id = continuation->connection->latest_stream_id + 1;

    if (aws_hash_table_put(
            &continuation->connection->continuation_table, &continuation->stream_id, continuation, NULL)) {
        continuation->stream_id = 0;
        goto clean_up;
    }

    /* The continuation table gets a ref count on the continuation. Take it here. */
    aws_event_stream_rpc_client_continuation_acquire(continuation);

    if (s_send_protocol_message(
            continuation->connection,
            continuation,
            &operation_name,
            message_args,
            continuation->stream_id,
            flush_fn,
            user_data)) {
        aws_hash_table_remove(&continuation->connection->continuation_table, &continuation->stream_id, NULL, NULL);
        continuation->stream_id = 0;
        goto clean_up;
    }

    continuation->connection->latest_stream_id = continuation->stream_id;
    ret_val = AWS_OP_SUCCESS;

clean_up:
    aws_mutex_unlock(&continuation->connection->stream_lock);
    return ret_val;
}

int aws_event_stream_rpc_client_continuation_send_message(
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data) {

    if (aws_event_stream_rpc_client_continuation_is_closed(continuation)) {
        return aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_STREAM_CLOSED);
    }

    if (!continuation->stream_id) {
        return aws_raise_error(AWS_ERROR_EVENT_STREAM_RPC_STREAM_NOT_ACTIVATED);
    }

    return s_send_protocol_message(
        continuation->connection, continuation, NULL, message_args, continuation->stream_id, flush_fn, user_data);
}
