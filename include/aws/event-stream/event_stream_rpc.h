#ifndef AWS_EVENT_STREAM_RPC_H
#define AWS_EVENT_STREAM_RPC_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/event-stream/event_stream.h>

extern AWS_EVENT_STREAM_API const struct aws_byte_cursor aws_event_stream_rpc_message_type_name;
extern AWS_EVENT_STREAM_API const struct aws_byte_cursor aws_event_stream_rpc_message_flags_name;
extern AWS_EVENT_STREAM_API const struct aws_byte_cursor aws_event_stream_rpc_stream_id_name;
extern AWS_EVENT_STREAM_API const struct aws_byte_cursor aws_event_stream_rpc_operation_name;

enum aws_event_stream_rpc_message_type {
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_MESSAGE,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_APPLICATION_ERROR,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PING,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PING_RESPONSE,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_CONNECT_ACK,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_PROTOCOL_ERROR,
    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_SERVER_ERROR,

    AWS_EVENT_STREAM_RPC_MESSAGE_TYPE_COUNT,
};

enum aws_event_stream_rpc_message_flag {
    AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_ACCEPTED = 1,
    AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_CONNECTION_REJECTED = 2,
    AWS_EVENT_STREAM_RPC_MESSAGE_FLAG_TERMINATE_STREAM = 4,
};

struct aws_event_stream_rpc_message_args {
    struct aws_event_stream_header_value_pair *headers;
    size_t headers_count;
    struct aws_byte_buf *payload;
    enum aws_event_stream_rpc_message_type message_type;
    uint32_t message_flags;
};

#endif /* AWS_EVENT_STREAM_RPC_SERVER_H */
