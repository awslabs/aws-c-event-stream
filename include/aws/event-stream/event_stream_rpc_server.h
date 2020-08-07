#ifndef AWS_EVENT_STREAM_RPC_SERVER_H
#define AWS_EVENT_STREAM_RPC_SERVER_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/event-stream/event_stream_rpc.h>

struct aws_channel;
struct aws_event_stream_rpc_connection;

struct aws_event_stream_rpc_server_continuation_token;

typedef void(aws_event_stream_rpc_server_stream_continuation_fn)(
    struct aws_event_stream_rpc_server_continuation_token *token,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data);
typedef void(aws_event_stream_rpc_server_stream_continuation_closed_fn)(
    struct aws_event_stream_rpc_server_continuation_token *token,
    void *user_data);

struct aws_event_stream_rpc_server_stream_continuation_options {
    aws_event_stream_rpc_server_stream_continuation_fn *on_continuation;
    aws_event_stream_rpc_server_stream_continuation_closed_fn *on_continuation_closed;
    void *user_data;
};

typedef void(aws_event_stream_rpc_server_connection_protocol_message_fn)(
    struct aws_event_stream_rpc_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data);
typedef void(aws_event_stream_rpc_server_on_incoming_stream_fn)(
    struct aws_event_stream_rpc_server_continuation_token *token,
    struct aws_byte_cursor operation_name,
    struct aws_event_stream_rpc_server_stream_continuation_options *continuation_options,
    void *user_data);

struct aws_event_stream_rpc_connection_options {
    aws_event_stream_rpc_server_on_incoming_stream_fn *on_incoming_stream;
    aws_event_stream_rpc_server_connection_protocol_message_fn *on_connection_protocol_message;
    void *user_data;
};

typedef int(aws_event_stream_rpc_server_on_new_connection_fn)(
    struct aws_event_stream_rpc_connection *connection,
    int error_code,
    struct aws_event_stream_rpc_connection_options *connection_options,
    void *user_data);
typedef void(aws_event_stream_rpc_server_on_connection_shutdown_fn)(
    struct aws_event_stream_rpc_connection *connection,
    int error_code,
    void *user_data);

typedef void(aws_event_stream_rpc_server_message_flush_fn)(int error_code, void *user_data);

struct aws_server_bootstrap;
struct aws_event_stream_rpc_server_listener;

typedef void(aws_event_stream_rpc_server_on_listener_destroy_fn)(
    struct aws_event_stream_rpc_server_listener *server,
    void *user_data);

struct aws_event_stream_rpc_server_listener_options {
    const char *host_name;
    uint16_t port;
    const struct aws_socket_options *socket_options;
    const struct aws_tls_connection_options *tls_options;
    struct aws_server_bootstrap *bootstrap;
    aws_event_stream_rpc_server_on_new_connection_fn *on_new_connection;
    aws_event_stream_rpc_server_on_connection_shutdown_fn *on_connection_shutdown;
    aws_event_stream_rpc_server_on_listener_destroy_fn *on_destroy_callback;
    void *user_data;
    bool enable_read_backpressure;
};

AWS_EXTERN_C_BEGIN

AWS_EVENT_STREAM_API struct aws_event_stream_rpc_server_listener *aws_event_stream_rpc_server_new_listener(
    struct aws_allocator *allocator,
    struct aws_event_stream_rpc_server_listener_options *options);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_listener_acquire(
    struct aws_event_stream_rpc_server_listener *listener);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_listener_release(
    struct aws_event_stream_rpc_server_listener *listener);

AWS_EVENT_STREAM_API struct aws_event_stream_rpc_connection *
    aws_event_stream_rpc_server_connection_from_existing_channel(
        struct aws_event_stream_rpc_server_listener *server,
        struct aws_channel *channel,
        const struct aws_event_stream_rpc_connection_options *connection_options);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_connection_acquire(
    struct aws_event_stream_rpc_connection *connection);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_connection_release(
    struct aws_event_stream_rpc_connection *connection);
AWS_EVENT_STREAM_API bool aws_event_stream_rpc_server_connection_is_closed(
    struct aws_event_stream_rpc_connection *connection);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_connection_close(
    struct aws_event_stream_rpc_connection *connection,
    int shutdown_error_code);
AWS_EVENT_STREAM_API int aws_event_stream_rpc_server_connection_send_protocol_message(
    struct aws_event_stream_rpc_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_server_message_flush_fn *flush_fn,
    void *user_data);

AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_continuation_acquire(
    struct aws_event_stream_rpc_server_continuation_token *continuation);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_server_continuation_release(
    struct aws_event_stream_rpc_server_continuation_token *continuation);
AWS_EVENT_STREAM_API bool aws_event_stream_rpc_server_continuation_is_closed(
    struct aws_event_stream_rpc_server_continuation_token *continuation);
AWS_EVENT_STREAM_API int aws_event_stream_rpc_server_continuation_send_message(
    struct aws_event_stream_rpc_server_continuation_token *continuation,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_server_message_flush_fn *flush_fn,
    void *user_data);

AWS_EXTERN_C_END

#endif /* AWS_EVENT_STREAM_RPC_SERVER_H */
