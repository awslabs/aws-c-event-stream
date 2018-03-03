/*
* Copyright 2010-2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License").
* You may not use this file except in compliance with the License.
* A copy of the License is located at
*
*  http://aws.amazon.com/apache2.0
*
* or in the "license" file accompanying this file. This file is distributed
* on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
* express or implied. See the License for the specific language governing
* permissions and limitations under the License.
*/
#include <aws/event-stream/event_stream.h>
#include <aws/common/encoding.h>
#include <stdlib.h>
#include <stdio.h>
#include <inttypes.h>

static void *mem_acquire_malloc(struct aws_allocator *alloc, size_t size) {
    return malloc(size);
}

static void mem_release_free(struct aws_allocator *alloc, void *ptr) {
    free(ptr);
}

static struct aws_allocator alloc = {
        .mem_acquire = mem_acquire_malloc,
        .mem_release = mem_release_free
};

static void on_payload_segment(struct aws_event_stream_streaming_decoder *decoder,
                               struct aws_byte_buf *data, int8_t final_segment, void *user_data) {

    if (data->len) {
        fwrite(data->buffer, sizeof(uint8_t), data->len, stdout);
    }

}

static void on_prelude_received(struct aws_event_stream_streaming_decoder *decoder,
                                struct aws_event_stream_message_prelude *prelude, void *user_data) {

    fprintf(stdout, "\n--------------------------------------------------------------------------------\n");
    fprintf(stdout, "total_length = 0x%08" PRIx32 "\nheaders_len = 0x%08" PRIx32 "\nprelude_crc = 0x%08" PRIx32 "\n\n",
            prelude->total_len, prelude->headers_len, prelude->prelude_crc);
}

static void on_header_received(struct aws_event_stream_streaming_decoder *decoder,
                               struct aws_event_stream_message_prelude *prelude,
                               struct aws_event_stream_header_value_pair *header, void *user_data) {
    fwrite(header->header_name, sizeof(uint8_t), (size_t) header->header_name_len, stdout);

    fprintf(stdout, ": ");

    if (header->header_value_type == AWS_EVENT_STREAM_HEADER_BOOL_FALSE) {
        fprintf(stdout, "false");
    } else if (header->header_value_type == AWS_EVENT_STREAM_HEADER_BOOL_TRUE) {
        fprintf(stdout, "true");
    } else if (header->header_value_type == AWS_EVENT_STREAM_HEADER_BYTE) {
        int8_t int_value = aws_event_stream_header_value_as_byte(header);
        fprintf(stdout, "%d", (int) int_value);
    } else if (header->header_value_type == AWS_EVENT_STREAM_HEADER_INT16) {
        int16_t int_value = aws_event_stream_header_value_as_int16(header);
        fprintf(stdout, "%d", (int) int_value);
    } else if (header->header_value_type == AWS_EVENT_STREAM_HEADER_INT32) {
        int32_t int_value = aws_event_stream_header_value_as_int32(header);
        fprintf(stdout, "%d", (int) int_value);
    } else if (header->header_value_type == AWS_EVENT_STREAM_HEADER_INT64 ||
               header->header_value_type == AWS_EVENT_STREAM_HEADER_TIMESTAMP) {
        int64_t int_value = aws_event_stream_header_value_as_int64(header);
        fprintf(stdout, "%lld", (long long) int_value);
    } else {
        if (header->header_value_type == AWS_EVENT_STREAM_HEADER_UUID) {
            struct aws_byte_buf uuid = aws_event_stream_header_value_as_uuid(header);
            fwrite(uuid.buffer, sizeof(uint8_t), uuid.len, stdout);
        } else {
            struct aws_byte_buf byte_buf = aws_event_stream_header_value_as_bytebuf(header);

            fwrite(byte_buf.buffer, sizeof(uint8_t), byte_buf.len, stdout);
        }
    }
    fprintf(stdout, "\n");
}

static void on_error(struct aws_event_stream_streaming_decoder *decoder,
                     struct aws_event_stream_message_prelude *prelude, int error_code, const char *message,
                     void *user_data) {

    fprintf(stderr, "Error encountered: Code: %d, Error Str: %s, Message: %s\n", error_code,
            aws_error_debug_str(error_code), message);
    exit(-1);
}

int main(void) {

    aws_load_error_strings();
    aws_event_stream_load_error_strings();

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init(&decoder, &alloc, on_payload_segment, on_prelude_received,
                                            on_header_received, on_error, NULL);

    setvbuf(stdin, NULL, _IONBF, 0);

    uint8_t data_buffer[1024];
    size_t read_val = 0;
    while ((read_val = fread(data_buffer, sizeof(uint8_t), sizeof(data_buffer), stdin))) {
        if (read_val > 0) {
            struct aws_byte_buf decode_data = aws_byte_buf_from_array(data_buffer, read_val);
            int err_code = aws_event_stream_streaming_decoder_pump(&decoder, &decode_data);
            if (err_code) {
                fprintf(stderr, "Error occurred during parsing. Error code: %d\n", err_code);
                aws_event_stream_streaming_decoder_clean_up(&decoder);
                return -1;
            }
            continue;
        }
        if (feof(stdin)) {
            fprintf(stdout, "\n");
            return 0;
        } else if (ferror(stdin)) {
            perror("Error reading from stdin\n");
            return ferror(stdin);
        }
    }

    return 0;
}