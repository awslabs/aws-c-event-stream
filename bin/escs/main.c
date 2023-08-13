/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/common/condition_variable.h>
#include <aws/common/mutex.h>
#include <aws/common/string.h>

#include <aws/io/channel_bootstrap.h>
#include <aws/io/event_loop.h>
#include <aws/io/socket.h>

#include <aws/event-stream/event_stream.h>
#include <aws/event-stream/event_stream_rpc_client.h>
#include <aws/event-stream/event_stream_rpc_server.h>

#ifdef _MSC_VER
#    pragma warning(disable : 4996) /* Disable warnings about fopen() being insecure */
#    pragma warning(disable : 4204) /* Declared initializers */
#    pragma warning(disable : 4221) /* Local var in declared initializer */
#endif

#ifdef WIN32
// Windows does not need specific imports
#else
#    include <stdio.h>
#endif

struct app_ctx {
    struct aws_allocator *allocator;
    struct aws_mutex lock;
    struct aws_condition_variable signal;

    struct aws_event_loop_group *elg;
    struct aws_host_resolver *resolver;
    struct aws_client_bootstrap *client_bootstrap;

    struct aws_server_bootstrap *server_bootstrap;
    struct aws_socket_endpoint endpoint;
    struct aws_socket_options socket_options;
    struct aws_event_stream_rpc_server_listener *listener;

    struct aws_event_stream_rpc_server_connection *server_connection;
    struct aws_event_stream_rpc_client_connection *client_connection;

    struct aws_event_stream_rpc_client_continuation_token *client_continuation;
    struct aws_event_stream_rpc_server_continuation_token *server_continuation;

    bool client_connection_setup_completed;
    bool server_connection_setup_completed;

    bool server_received_connect;
    bool client_received_connack;

    bool client_continuation_message_received;

    bool client_connection_shutdown_completed;
    bool server_connection_shutdown_completed;
    bool server_listener_shutdown_completed;
};

static bool s_shutdown_predicate(void *arg) {
    struct app_ctx *context = arg;
    return context->server_listener_shutdown_completed && context->server_connection_shutdown_completed && context->client_connection_shutdown_completed;
}

static bool s_setup_completed_predicate(void *arg) {
    struct app_ctx *context = arg;
    return context->client_connection_setup_completed && context->server_connection_setup_completed;
}

static void s_fixture_on_server_protocol_message(
    struct aws_event_stream_rpc_server_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    (void)connection;

    struct app_ctx *context = user_data;
    aws_mutex_lock(&context->lock);

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT) {
        context->server_received_connect = true;
    }

    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_aws_event_stream_rpc_server_message_flush_fn(int error_code, void *user_data) {
    (void)error_code;
    (void)user_data;
}

static void s_on_stream_server_continuation_shim(
    struct aws_event_stream_rpc_server_continuation_token *token,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {

    aws_event_stream_rpc_server_continuation_send_message(token, message_args, s_aws_event_stream_rpc_server_message_flush_fn, user_data);
}

static void s_stream_server_continuation_closed_shim(
    struct aws_event_stream_rpc_server_continuation_token *token,
    void *user_data) {
    (void)token;
    (void)user_data;

    // TODO
}

static int s_on_server_incoming_stream_shim(
    struct aws_event_stream_rpc_server_connection *connection,
    struct aws_event_stream_rpc_server_continuation_token *token,
    struct aws_byte_cursor operation_name,
    struct aws_event_stream_rpc_server_stream_continuation_options *continuation_options,
    void *user_data) {
    (void)connection;
    (void)token;
    (void)operation_name;

    struct app_ctx *context = user_data;

    continuation_options->on_continuation = s_on_stream_server_continuation_shim;
    continuation_options->on_continuation_closed = s_stream_server_continuation_closed_shim;
    continuation_options->user_data = context;

    context->server_continuation = token;

    return AWS_OP_SUCCESS;
}

static int s_on_new_server_connection(
    struct aws_event_stream_rpc_server_connection *connection,
    int error_code,
    struct aws_event_stream_rpc_connection_options *connection_options,
    void *user_data) {
    (void)error_code;

    struct app_ctx *context = user_data;
    context->server_connection = connection;
    aws_mutex_lock(&context->lock);
    context->server_connection_setup_completed = true;
    aws_mutex_unlock(&context->lock);
    aws_event_stream_rpc_server_connection_acquire(connection);

    connection_options->on_connection_protocol_message = s_fixture_on_server_protocol_message;
    connection_options->on_incoming_stream = s_on_server_incoming_stream_shim;
    connection_options->user_data = user_data;

    aws_condition_variable_notify_one(&context->signal);

    return AWS_OP_SUCCESS;
}

static void s_on_server_connection_shutdown(
    struct aws_event_stream_rpc_server_connection *connection,
    int error_code,
    void *user_data) {

    (void)connection;
    (void)error_code;

    struct app_ctx *context = user_data;
    context->server_connection = NULL;
    aws_mutex_lock(&context->lock);
    context->server_connection_shutdown_completed = true;
    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_on_server_listener_destroy(struct aws_event_stream_rpc_server_listener *server, void *user_data) {
    (void)server;
    struct app_ctx *context = user_data;

    aws_mutex_lock(&context->lock);
    context->server_listener_shutdown_completed = true;
    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_client_on_connection_setup(
    struct aws_event_stream_rpc_client_connection *connection,
    int error_code,
    void *user_data) {
    (void)error_code;
    struct app_ctx *context = user_data;

    aws_mutex_lock(&context->lock);
    context->client_connection = connection;

    if (connection) {
        aws_event_stream_rpc_client_connection_acquire(connection);
    }
    context->client_connection_setup_completed = true;
    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_client_on_connection_shutdown(
    struct aws_event_stream_rpc_client_connection *connection,
    int error_code,
    void *user_data) {
    (void)connection;
    (void)error_code;

    struct app_ctx *context = user_data;

    aws_mutex_lock(&context->lock);
    context->client_connection_shutdown_completed = true;
    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_client_connection_protocol_message(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    (void)connection;

    struct app_ctx *context = user_data;
    aws_mutex_lock(&context->lock);

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK) {
        context->client_received_connack = true;
    }

    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_init_app_ctx(struct app_ctx *context, struct aws_allocator *allocator) {
    AWS_ZERO_STRUCT(*context);

    context->allocator = allocator;
    context->signal = (struct aws_condition_variable)AWS_CONDITION_VARIABLE_INIT;
    aws_mutex_init(&context->lock);

    context->elg = aws_event_loop_group_new_default(allocator, 1, NULL);

    struct aws_host_resolver_default_options resolver_options = {
        .el_group = context->elg,
        .max_entries = 8,
    };
    context->resolver = aws_host_resolver_new_default(allocator, &resolver_options);

    struct aws_client_bootstrap_options client_bootstrap_options = {
        .event_loop_group = context->elg,
        .host_resolver = context->resolver,
    };
    context->client_bootstrap = aws_client_bootstrap_new(allocator, &client_bootstrap_options);
    context->server_bootstrap = aws_server_bootstrap_new(allocator, context->elg);

    struct aws_socket_options socket_options = {
        .connect_timeout_ms = 1000,
        .domain = AWS_SOCKET_LOCAL,
    };

    context->socket_options = socket_options;

    aws_socket_endpoint_init_local_address_for_test(&context->endpoint);

    struct aws_event_stream_rpc_server_listener_options listener_options = {
        .socket_options = &context->socket_options,
        .host_name = context->endpoint.address,
        .port = context->endpoint.port,
        .bootstrap = context->server_bootstrap,
        .user_data = context,
        .on_new_connection = s_on_new_server_connection,
        .on_connection_shutdown = s_on_server_connection_shutdown,
        .on_destroy_callback = s_on_server_listener_destroy,
    };

    context->listener = aws_event_stream_rpc_server_new_listener(allocator, &listener_options);

    struct aws_event_stream_rpc_client_connection_options connection_options = {
        .socket_options = &context->socket_options,
        .user_data = context,
        .bootstrap = context->client_bootstrap,
        .host_name = context->endpoint.address,
        .port = context->endpoint.port,
        .on_connection_setup = s_client_on_connection_setup,
        .on_connection_shutdown = s_client_on_connection_shutdown,
        .on_connection_protocol_message = s_client_connection_protocol_message,
    };

    aws_event_stream_rpc_client_connection_connect(allocator, &connection_options);

    aws_mutex_lock(&context->lock);
    aws_condition_variable_wait_pred(&context->signal, &context->lock, s_setup_completed_predicate, context);
    aws_mutex_unlock(&context->lock);
}

static void s_clean_up_app_ctx(struct app_ctx *context) {

    aws_event_stream_rpc_client_connection_close(context->client_connection, AWS_ERROR_SUCCESS);
    aws_event_stream_rpc_server_connection_close(context->server_connection, AWS_ERROR_SUCCESS);

    aws_event_stream_rpc_client_connection_release(context->client_connection);
    aws_event_stream_rpc_server_connection_release(context->server_connection);
    aws_event_stream_rpc_server_listener_release(context->listener);

    aws_mutex_lock(&context->lock);
    aws_condition_variable_wait_pred(
        &context->signal, &context->lock, s_shutdown_predicate, context);
    aws_mutex_unlock(&context->lock);

    aws_server_bootstrap_release(context->server_bootstrap);
    aws_client_bootstrap_release(context->client_bootstrap);
    aws_host_resolver_release(context->resolver);
    aws_event_loop_group_release(context->elg);

    aws_thread_join_all_managed();
}

static void s_rpc_client_message_flush(int error_code, void *user_data) {
    (void)error_code;
    (void)user_data;
}

static void s_rpc_server_message_flush(int error_code, void *user_data) {
    (void)error_code;
    (void)user_data;
}

static bool s_server_received_connect_predicate(void *arg) {
    struct app_ctx *context = arg;
    return context->server_received_connect;
}

static bool s_client_received_connack_predicate(void *arg) {
    struct app_ctx *context = arg;
    return context->client_received_connack;
}

static void s_connect_handshake(struct app_ctx *context) {
    struct aws_byte_buf connect_payload = aws_byte_buf_from_c_str("{ \"message\": \" connect message \" }");
    struct aws_event_stream_rpc_message_args connect_args = {
        .headers_count = 0,
        .headers = NULL,
        .message_type = AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT,
        .payload = &connect_payload,
    };

    aws_event_stream_rpc_client_connection_send_protocol_message(
        context->client_connection, &connect_args, s_rpc_client_message_flush, context);

    aws_mutex_lock(&context->lock);
    aws_condition_variable_wait_pred(
        &context->signal,
        &context->lock,
        s_server_received_connect_predicate,
        context);
    aws_mutex_unlock(&context->lock);

    struct aws_byte_buf connack_payload = aws_byte_buf_from_c_str("{ \"message\": \" connack message \" }");
    struct aws_event_stream_rpc_message_args connack_args = {
        .headers_count = 0,
        .headers = NULL,
        .message_type = AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK,
        .message_flags = AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_ACCEPTED,
        .payload = &connack_payload,
    };

    aws_event_stream_rpc_server_connection_send_protocol_message(
        context->server_connection, &connack_args, s_rpc_server_message_flush, context);

    aws_mutex_lock(&context->lock);
    aws_condition_variable_wait_pred(
        &context->signal,
        &context->lock,
        s_client_received_connack_predicate,
        context);
    aws_mutex_unlock(&context->lock);
}

static void s_rpc_client_stream_continuation(
    struct aws_event_stream_rpc_client_continuation_token *token,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data) {
    (void)token;

    struct app_ctx *context = user_data;

    aws_mutex_lock(&context->lock);

    if (message_args->message_type == AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE) {
        context->client_continuation_message_received = true;
    }

    aws_mutex_unlock(&context->lock);
    aws_condition_variable_notify_one(&context->signal);
}

static void s_rpc_client_stream_continuation_closed(
    struct aws_event_stream_rpc_client_continuation_token *token,
    void *user_data) {

    (void)token;
    (void)user_data;
}

static bool s_client_continuation_message_recceived_predicate(void *arg) {
    struct app_ctx *context = arg;
    return context->client_continuation_message_received;
}

static void s_operation(struct app_ctx *context) {
    struct aws_event_stream_rpc_client_stream_continuation_options continuation_options = {
        .user_data = context,
        .on_continuation = s_rpc_client_stream_continuation,
        .on_continuation_closed = s_rpc_client_stream_continuation_closed,
    };

    context->client_continuation =
        aws_event_stream_rpc_client_connection_new_stream(context->client_connection, &continuation_options);

    struct aws_byte_cursor operation_name = aws_byte_cursor_from_c_str("test_operation");
    struct aws_byte_buf operation_payload = aws_byte_buf_from_c_str("{ \"message\": \" operation payload \" }");
    struct aws_event_stream_rpc_message_args operation_args = {
        .headers_count = 0,
        .headers = NULL,
        .message_type = AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE,
        .payload = &operation_payload,
    };

    aws_event_stream_rpc_client_continuation_activate(
        context->client_continuation, operation_name, &operation_args, s_rpc_client_message_flush, context);

    aws_mutex_lock(&context->lock);
    aws_condition_variable_wait_pred(
        &context->signal,
        &context->lock,
        s_client_continuation_message_recceived_predicate,
        context);
    aws_mutex_unlock(&context->lock);

    aws_event_stream_rpc_client_continuation_release(context->client_continuation);
}

int main(int argc, char **argv) {
    (void)argc;
    (void)argv;

    struct aws_allocator *allocator = aws_default_allocator();

    aws_event_stream_library_init(allocator);

    struct app_ctx context;
    s_init_app_ctx(&context, allocator);

    s_connect_handshake(&context);

    s_operation(&context);

    s_clean_up_app_ctx(&context);

    aws_event_stream_library_clean_up();

    return 0;
}
