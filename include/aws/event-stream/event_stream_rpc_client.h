#ifndef AWS_EVENT_STREAM_RPC_CLIENT_H
#define AWS_EVENT_STREAM_RPC_CLIENT_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/event-stream/event_stream_rpc.h>

struct aws_channel;
struct aws_event_stream_rpc_client_connection;
struct aws_event_stream_rpc_client_continuation_token;

/**
 * Invoked when a connection receives a message on an existing stream. message_args contains the
 * message data.
 */
typedef void(aws_event_stream_rpc_client_stream_continuation_fn)(
    struct aws_event_stream_rpc_client_continuation_token *token,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data);

/**
 * Invoked when a continuation has either been closed with the TERMINATE_STREAM flag, or when the connection
 * shuts down and deletes the continuation.
 */
typedef void(aws_event_stream_rpc_client_stream_continuation_closed_fn)(
    struct aws_event_stream_rpc_client_continuation_token *token,
    void *user_data);

struct aws_event_stream_rpc_client_stream_continuation_options {
    aws_event_stream_rpc_client_stream_continuation_fn *on_continuation;
    aws_event_stream_rpc_client_stream_continuation_closed_fn *on_continuation_closed;
    void *user_data;
};

/**
 * Invoked when a non-stream level message is received on a connection.
 */
typedef void(aws_event_stream_rpc_client_connection_protocol_message_fn)(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    void *user_data);

/**
 * Invoked when a successfully created connection is shutdown. error_code will indicate the reason for the shutdown.
 */
typedef void(aws_event_stream_rpc_client_on_connection_shutdown_fn)(
    struct aws_event_stream_rpc_client_connection *connection,
    int error_code,
    void *user_data);

typedef int(aws_event_stream_rpc_client_on_connection_setup_fn)(
    struct aws_event_stream_rpc_client_connection *connection,
    int error_code,
    void *user_data);

/**
 * Invoked whenever a message has been flushed to the channel.
 */
typedef void(aws_event_stream_rpc_client_message_flush_fn)(int error_code, void *user_data);

struct aws_client_bootstrap;

struct aws_event_stream_rpc_client_connection_options {
    /** host name to use for the connection. This depends on your socket type. */
    const char *host_name;
    /** port to use for your connection, assuming for the appropriate socket type. */
    uint16_t port;
    const struct aws_socket_options *socket_options;
    /** optional: tls options for using when establishing your connection. */
    const struct aws_tls_connection_options *tls_options;
    struct aws_client_bootstrap *bootstrap;
    aws_event_stream_rpc_client_on_connection_setup_fn *on_connection_setup;
    aws_event_stream_rpc_client_connection_protocol_message_fn *on_connection_protocol_message;
    aws_event_stream_rpc_client_on_connection_shutdown_fn *on_connection_shutdown;
    void *user_data;
};

AWS_EXTERN_C_BEGIN

AWS_EVENT_STREAM_API int aws_event_stream_rpc_client_connection_connect(
    struct aws_allocator *allocator,
    const struct aws_event_stream_rpc_client_connection_options *conn_options);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_client_connection_release(
    struct aws_event_stream_rpc_client_connection *connection);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_client_connection_close(
    struct aws_event_stream_rpc_client_connection *connection,
    int shutdown_error_code);

AWS_EVENT_STREAM_API int aws_event_stream_rpc_client_connection_send_protocol_message(
    struct aws_event_stream_rpc_client_connection *connection,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data);

AWS_EVENT_STREAM_API struct aws_event_stream_rpc_client_continuation_token *
    aws_event_stream_rpc_client_connection_new_stream(
        struct aws_event_stream_rpc_client_connection *connection,
        const struct aws_event_stream_rpc_client_stream_continuation_options *continuation_options);
AWS_EVENT_STREAM_API void aws_event_stream_rpc_client_continuation_release(
    struct aws_event_stream_rpc_client_continuation_token *continuation);

AWS_EVENT_STREAM_API int aws_event_stream_rpc_client_continuation_activate(
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data);
AWS_EVENT_STREAM_API int aws_event_stream_rpc_client_continuation_send_message(
    struct aws_event_stream_rpc_client_continuation_token *continuation,
    const struct aws_event_stream_rpc_message_args *message_args,
    aws_event_stream_rpc_client_message_flush_fn *flush_fn,
    void *user_data);

AWS_EXTERN_C_END

#endif /* AWS_EVENT_STREAM_RPC_CLIENT_H */
