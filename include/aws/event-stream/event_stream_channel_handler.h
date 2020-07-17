#ifndef AWS_EVENT_STREAM_CHANNEL_HANDLER_H
#define AWS_EVENT_STREAM_CHANNEL_HANDLER_H
/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/event-stream/event_stream.h>
#include <aws/io/channel.h>

struct aws_event_stream_channel_handler;

/**
 * Invoked when an aws_event_stream_message is encountered. If the message
 * parsed successfully, message will be non-null and error_code will be AWS_OP_SUCCESS.
 * Otherwiuse message will be null and error_code will represent the error that was encountered.
 * Note that any case that error_code was not AWS_OP_SUCCESS, the channel also shutsdown.
 */
typedef void(aws_event_stream_channel_handler_on_message_received_fn)(
    struct aws_event_stream_message *message,
    int error_code,
    void *user_data);

/**
 * Invoked when an aws_event_stream_message is flushed to the IO interface. When error_code is AWS_OP_SUCCESS the
 * write happened successfuly. Regardless, message is held from the aws_event_stream_channel_handler_write_message()
 * call and should likely be freed in this callback.
 */
typedef void(aws_event_stream_channel_handler_on_message_written_fn)(
    struct aws_event_stream_message *message,
    int error_code,
    void *user_data);

struct aws_event_stream_channel_handler_options {
    /** Callback for when messages are received. Can not be null. */
    aws_event_stream_channel_handler_on_message_received_fn *on_message_received;
    void *user_data;
    /** initial window size to use for the channel. If automatic window management is set to true, this value is
     * ignored. */
    size_t initial_window_size;
    /**
     * if set to true, windowing will be managed automatically for the user. Otherwise, after any on_message_received,
     * the user must invoke aws_event_stream_channel_handler_increment_read_window()
     */
    bool automatic_window_management;
};

AWS_EXTERN_C_BEGIN
/**
 * Allocates and initializes a new channel handler for processing aws_event_stream_message() events. Handler options
 * must not be null.
 */
AWS_EVENT_STREAM_API struct aws_channel_handler *aws_event_stream_channel_handler_new(
    struct aws_allocator *allocator,
    const struct aws_event_stream_channel_handler_options *handler_options);

/**
 * Writes an aws_event_stream_message() to the channel. Once the channel flushes or an error ocdcurs, on_message_written
 * will be invoked. message should stay valid until the callback is invoked.
 */
AWS_EVENT_STREAM_API int aws_event_stream_channel_handler_write_message(
    struct aws_channel_handler *handler,
    struct aws_event_stream_message *message,
    aws_event_stream_channel_handler_on_message_written_fn *on_message_written,
    void *user_data);

/**
 * Updates the read window for the channel if automatic_window_managemanet was set to false.
 */
AWS_EVENT_STREAM_API int aws_event_stream_channel_handler_increment_read_window(
    struct aws_channel_handler *handler,
    size_t window_update_size);

AWS_EXTERN_C_END

#endif /* AWS_EVENT_STREAM_CHANNEL_HANDLER_H */
