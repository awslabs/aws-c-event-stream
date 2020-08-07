/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */
#include <aws/event-stream/event_stream_rpc.h>

const struct aws_byte_cursor aws_event_stream_rpc_message_type_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(":message-type");
const struct aws_byte_cursor aws_event_stream_rpc_message_flags_name =
    AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(":message-flags");
const struct aws_byte_cursor aws_event_stream_rpc_stream_id_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL(":stream-id");
const struct aws_byte_cursor aws_event_stream_rpc_operation_name = AWS_BYTE_CUR_INIT_FROM_STRING_LITERAL("operation");
