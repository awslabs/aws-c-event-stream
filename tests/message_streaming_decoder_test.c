/**
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0.
 */

#include <aws/checksums/crc.h>
#include <aws/common/array_list.h>
#include <aws/event-stream/event_stream.h>
#include <aws/testing/aws_test_harness.h>

struct test_decoder_data {
    struct aws_event_stream_message_prelude latest_prelude;
    struct aws_array_list headers_list;
    uint8_t *latest_payload;
    size_t written;
    struct aws_allocator *alloc;
    int latest_error;
    uint32_t message_crc;
};

static void s_decoder_test_on_payload_segment(
    struct aws_event_stream_streaming_decoder *decoder,
    struct aws_byte_buf *data,
    int8_t final_segment,
    void *user_data) {
    (void)final_segment;
    (void)decoder;
    struct test_decoder_data *decoder_data = (struct test_decoder_data *)user_data;
    memcpy(decoder_data->latest_payload + decoder_data->written, data->buffer, data->len);
    decoder_data->written += data->len;
}

static void s_decoder_test_on_prelude_received(
    struct aws_event_stream_streaming_decoder *decoder,
    struct aws_event_stream_message_prelude *prelude,
    void *user_data) {

    (void)decoder;
    struct test_decoder_data *decoder_data = (struct test_decoder_data *)user_data;
    decoder_data->latest_prelude = *prelude;

    if (decoder_data->latest_payload) {
        aws_mem_release(decoder_data->alloc, decoder_data->latest_payload);
    }

    const size_t payload_size = decoder_data->latest_prelude.total_len - AWS_EVENT_STREAM_PRELUDE_LENGTH -
                                AWS_EVENT_STREAM_TRAILER_LENGTH - decoder_data->latest_prelude.headers_len;

    if (payload_size) {
        decoder_data->latest_payload = aws_mem_acquire(decoder_data->alloc, payload_size);
    } else {
        decoder_data->latest_payload = NULL;
    }
    decoder_data->written = 0;
}

static void s_decoder_test_header_received(
    struct aws_event_stream_streaming_decoder *decoder,
    struct aws_event_stream_message_prelude *prelude,
    struct aws_event_stream_header_value_pair *header,
    void *user_data) {
    (void)decoder;
    (void)prelude;
    struct test_decoder_data *decoder_data = (struct test_decoder_data *)user_data;
    aws_event_stream_add_header(&decoder_data->headers_list, header);
}

static void s_decoder_test_on_complete(
    struct aws_event_stream_streaming_decoder *decoder,
    uint32_t message_crc,
    void *user_data) {
    (void)decoder;
    struct test_decoder_data *decoder_data = (struct test_decoder_data *)user_data;
    decoder_data->message_crc = message_crc;
}

static void s_decoder_test_on_error(
    struct aws_event_stream_streaming_decoder *decoder,
    struct aws_event_stream_message_prelude *prelude,
    int error_code,
    const char *message,
    void *user_data) {

    (void)decoder;
    (void)prelude;
    (void)message;
    struct test_decoder_data *decoder_data = (struct test_decoder_data *)user_data;
    decoder_data->latest_error = error_code;
}

static int s_test_streaming_decoder_incoming_no_op_valid_single_message_fn(struct aws_allocator *allocator, void *ctx) {
    uint8_t test_data[] = {
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
        0x00,
        0x00,
        0x00,
        0x05,
        0xc2,
        0x48,
        0xeb,
        0x7d,
        0x98,
        0xc8,
        0xff,
    };

    (void)ctx;
    struct test_decoder_data decoder_data = {.latest_payload = 0, .written = 0, .alloc = allocator, .latest_error = 0};

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));
    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x00000010, decoder_data.latest_prelude.total_len, "Message length should have been 0x10");
    ASSERT_INT_EQUALS(0x00000000, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0x00");
    ASSERT_INT_EQUALS(0x05c248eb, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0x8c335472");
    ASSERT_INT_EQUALS(0, decoder_data.written, "No payload data should have been written");

    if (decoder_data.latest_payload) {
        aws_mem_release(allocator, decoder_data.latest_payload);
    }
    ASSERT_UINT_EQUALS(0x7D98C8FF, decoder_data.message_crc);

    aws_event_stream_streaming_decoder_clean_up(&decoder);

    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_no_op_valid_single_message,
    s_test_streaming_decoder_incoming_no_op_valid_single_message_fn)

static int s_test_streaming_decoder_incoming_application_no_headers_fn(struct aws_allocator *allocator, void *ctx) {
    uint8_t test_data[] = {
        0x00, 0x00, 0x00, 0x1D, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x52, 0x8c, 0x5a, 0x7b, 0x27, 0x66,
        0x6f, 0x6f, 0x27, 0x3a, 0x27, 0x62, 0x61, 0x72, 0x27, 0x7d, 0xc3, 0x65, 0x39, 0x36,
    };

    (void)ctx;
    struct test_decoder_data decoder_data = {.latest_payload = 0, .written = 0, .alloc = allocator, .latest_error = 0};

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));

    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x0000001D, decoder_data.latest_prelude.total_len, "Message length should have been 0x1D");
    ASSERT_INT_EQUALS(0x00000000, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0x00");
    ASSERT_INT_EQUALS(0xfd528c5a, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0xfd528c5a");

    const char *expected_str = "{'foo':'bar'}";
    size_t payload_len = decoder_data.latest_prelude.total_len - AWS_EVENT_STREAM_PRELUDE_LENGTH -
                         AWS_EVENT_STREAM_TRAILER_LENGTH - decoder_data.latest_prelude.headers_len;
    ASSERT_INT_EQUALS(
        strlen(expected_str), payload_len, "payload length should have been %d", (int)(strlen(expected_str)));

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_str,
        strlen(expected_str),
        decoder_data.latest_payload,
        payload_len,
        "payload should have been %s",
        expected_str);

    if (decoder_data.latest_payload) {
        aws_mem_release(allocator, decoder_data.latest_payload);
    }
    ASSERT_UINT_EQUALS(0xC3653936, decoder_data.message_crc);

    aws_event_stream_streaming_decoder_clean_up(&decoder);

    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_application_no_headers,
    s_test_streaming_decoder_incoming_application_no_headers_fn)

static int s_test_streaming_decoder_incoming_application_one_compressed_header_pair_valid_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;
    uint8_t test_data[] = {
        0x00, 0x00, 0x00, 0x3D, 0x00, 0x00, 0x00, 0x20, 0x07, 0xFD, 0x83, 0x96, 0x0C, 'c',  'o',  'n',
        't',  'e',  'n',  't',  '-',  't',  'y',  'p',  'e',  0x07, 0x00, 0x10, 'a',  'p',  'p',  'l',
        'i',  'c',  'a',  't',  'i',  'o',  'n',  '/',  'j',  's',  'o',  'n',  0x7b, 0x27, 0x66, 0x6f,
        0x6f, 0x27, 0x3a, 0x27, 0x62, 0x61, 0x72, 0x27, 0x7d, 0x8D, 0x9C, 0x08, 0xB1,
    };

    struct test_decoder_data decoder_data = {
        .latest_payload = 0,
        .written = 0,
        .alloc = allocator,
        .latest_error = 0,
    };
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));

    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x0000003D, decoder_data.latest_prelude.total_len, "Message length should have been 0x3D");
    ASSERT_INT_EQUALS(0x00000020, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0x20");
    ASSERT_INT_EQUALS(0x07FD8396, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0x07FD8396");

    const char *content_type = "content-type";
    const char *content_type_value = "application/json";

    struct aws_event_stream_header_value_pair latest_header;
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 0);
    struct aws_byte_buf latest_header_value = aws_event_stream_header_value_as_string(&latest_header);

    ASSERT_BIN_ARRAYS_EQUALS(
        content_type,
        strlen(content_type),
        latest_header.header_name,
        latest_header.header_name_len,
        "header name should have been %s",
        content_type);
    ASSERT_BIN_ARRAYS_EQUALS(
        content_type_value,
        strlen(content_type_value),
        latest_header_value.buffer,
        latest_header_value.len,
        "header value should have been %s",
        content_type_value);

    const char *expected_str = "{'foo':'bar'}";
    size_t payload_len = decoder_data.latest_prelude.total_len - AWS_EVENT_STREAM_PRELUDE_LENGTH -
                         AWS_EVENT_STREAM_TRAILER_LENGTH - decoder_data.latest_prelude.headers_len;
    ASSERT_INT_EQUALS(
        strlen(expected_str), payload_len, "payload length should have been %d", (int)(strlen(expected_str)));

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_str,
        strlen(expected_str),
        decoder_data.latest_payload,
        payload_len,
        "payload should have been %s",
        expected_str);

    if (decoder_data.latest_payload) {
        aws_mem_release(allocator, decoder_data.latest_payload);
    }
    ASSERT_UINT_EQUALS(0x8D9C08B1, decoder_data.message_crc);

    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);
    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_application_one_compressed_header_pair_valid,
    s_test_streaming_decoder_incoming_application_one_compressed_header_pair_valid_fn)

static int s_test_streaming_decoder_incoming_application_one_int32_header_pair_valid_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;
    /* clang-format off */
    uint8_t test_data[] = {
        0x00, 0x00, 0x00, 0x1b,         /* total length */
        0x00, 0x00, 0x00, 0x0b,         /* headers length */
        0xe5, 0xc0, 0xa0, 0x72,         /* prelude crc */
        0x05,                           /* header name length */
        'e',  'v',  'e',  'n',  't',    /* header name */
        0x04,                           /* header value type */
        0x00, 0x00,                     /* header value length */
        0x00, 0x20,                     /* header value */
        0x04, 0xa1, 0xd4, 0x7c          /* message crc */
    };
    /* clang-format on */

    struct test_decoder_data decoder_data = {
        .latest_payload = 0,
        .written = 0,
        .alloc = allocator,
        .latest_error = 0,
    };
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));

    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x0000001B, decoder_data.latest_prelude.total_len, "Message length should have been 0x1B");
    ASSERT_INT_EQUALS(0x0000000B, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0xB");
    ASSERT_INT_EQUALS(0xE5C0A072, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0xE5C0A072");

    const char *expected_header_name = "event";
    struct aws_event_stream_header_value_pair latest_header;
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 0);

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_header_name,
        strlen(expected_header_name),
        latest_header.header_name,
        latest_header.header_name_len,
        "header name should have been %s",
        expected_header_name);

    int32_t latest_header_value = aws_event_stream_header_value_as_int32(&latest_header);
    ASSERT_INT_EQUALS(0x00000020, latest_header_value, "Header value should have been 0x00000020");
    ASSERT_UINT_EQUALS(0x04A1D47C, decoder_data.message_crc);

    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);
    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_application_one_int32_header_pair_valid,
    s_test_streaming_decoder_incoming_application_one_int32_header_pair_valid_fn)

static int s_test_streaming_decoder_incoming_application_variable_headers_with_empty_length_pair_valid_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;
    /* clang-format off */
    uint8_t test_data[] = {
        0x00, 0x00, 0x00, 0x22,         /* total length */
        0x00, 0x00, 0x00, 0x12,         /* headers length */
        0x2D, 0x9A, 0xD2, 0x45,         /* prelude crc */
        0x04,                           /* header name length */
        'b',  'u',  'f',  'f',          /* header name */
        0x06,                           /* header value type (BYTE ARRAY)*/
        0x00, 0x00,                     /* header value length */
        0x06,                           /* header name length */
        's',  't',  'r',  'i','n','g',  /* header name */
        0x07,                           /* header value type (String)*/
        0x00, 0x00,                     /* header value length */
        0xC8, 0x4C, 0xF8, 0x53          /* message crc */
    };
    /* clang-format on */
    struct test_decoder_data decoder_data = {
        .latest_payload = 0,
        .written = 0,
        .alloc = allocator,
        .latest_error = 0,
    };
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);
    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));
    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x00000022, decoder_data.latest_prelude.total_len);
    ASSERT_INT_EQUALS(0x00000012, decoder_data.latest_prelude.headers_len);
    ASSERT_INT_EQUALS(0x2D9AD245, decoder_data.latest_prelude.prelude_crc);
    ASSERT_UINT_EQUALS(0xC84CF853, decoder_data.message_crc);

    const char *expected_header_name = "buff";
    struct aws_event_stream_header_value_pair latest_header;
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 0);

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_header_name,
        strlen(expected_header_name),
        latest_header.header_name,
        latest_header.header_name_len,
        "header name should have been %s",
        expected_header_name);

    struct aws_byte_buf latest_header_value = aws_event_stream_header_value_as_bytebuf(&latest_header);
    ASSERT_INT_EQUALS(0, latest_header_value.len);
    ASSERT_NULL(latest_header_value.buffer);

    const char *expected_string_header_name = "string";
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 1);

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_string_header_name,
        strlen(expected_string_header_name),
        latest_header.header_name,
        latest_header.header_name_len,
        "header name should have been %s",
        expected_header_name);

    latest_header_value = aws_event_stream_header_value_as_bytebuf(&latest_header);
    ASSERT_INT_EQUALS(0, latest_header_value.len);
    ASSERT_NULL(latest_header_value.buffer);

    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);
    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_application_variable_headers_with_empty_length_pair_valid,
    s_test_streaming_decoder_incoming_application_variable_headers_with_empty_length_pair_valid_fn)

static int s_test_streaming_decoder_incoming_application_one_bool_header_pair_valid_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;
    /* clang-format off */
    uint8_t test_data[] = {
        0x00, 0x00, 0x00, 0x17,         /* total length */
        0x00, 0x00, 0x00, 0x07,         /* headers length */
        0x29, 0x86, 0x01, 0x58,         /* prelude crc */
        0x05,                           /* header name length */
        'e',  'v',  'e',  'n',  't',    /* header name */
        0x00,                           /* header value type */
        0x4b, 0x4d, 0x2b, 0xe7          /* message crc */
    };
    /* clang-format on */

    struct test_decoder_data decoder_data = {
        .latest_payload = 0,
        .written = 0,
        .alloc = allocator,
        .latest_error = 0,
    };
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));

    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x00000017, decoder_data.latest_prelude.total_len, "Message length should have been 0x17");
    ASSERT_INT_EQUALS(0x00000007, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0x7");
    ASSERT_INT_EQUALS(0x29860158, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0x29860158");

    const char *expected_header_name = "event";
    struct aws_event_stream_header_value_pair latest_header;
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 0);

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_header_name,
        strlen(expected_header_name),
        latest_header.header_name,
        latest_header.header_name_len,
        "header name should have been %s",
        expected_header_name);

    int8_t latest_header_value = aws_event_stream_header_value_as_bool(&latest_header);
    ASSERT_INT_EQUALS(1, latest_header_value, "Header value should have been true");
    ASSERT_UINT_EQUALS(0x4B4D2BE7, decoder_data.message_crc);

    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);
    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_application_one_bool_header_pair_valid,
    s_test_streaming_decoder_incoming_application_one_bool_header_pair_valid_fn)

static int s_test_streaming_decoder_incoming_multiple_messages_fn(struct aws_allocator *allocator, void *ctx) {
    (void)ctx;
    uint8_t test_data[] = {
        /* message 1 */
        0x00,
        0x00,
        0x00,
        0x10,
        0x00,
        0x00,
        0x00,
        0x00,
        0x05,
        0xc2,
        0x48,
        0xeb,
        0x7d,
        0x98,
        0xc8,
        0xff,
        /* message 2 */
        0x00,
        0x00,
        0x00,
        0x3D,
        0x00,
        0x00,
        0x00,
        0x20,
        0x07,
        0xFD,
        0x83,
        0x96,
        0x0C,
        'c',
        'o',
        'n',
        't',
        'e',
        'n',
        't',
        '-',
        't',
        'y',
        'p',
        'e',
        0x07,
        0x00,
        0x10,
        'a',
        'p',
        'p',
        'l',
        'i',
        'c',
        'a',
        't',
        'i',
        'o',
        'n',
        '/',
        'j',
        's',
        'o',
        'n',
        0x7b,
        0x27,
        0x66,
        0x6f,
        0x6f,
        0x27,
        0x3a,
        0x27,
        0x62,
        0x61,
        0x72,
        0x27,
        0x7d,
        0x8D,
        0x9C,
        0x08,
        0xB1,
    };

    size_t first_message_size = 0x10;
    size_t read_size = 7; /* make this a weird number to force edge case coverage in the parser.
                                This will fall into the middle of message boundaries and preludes. */

    struct test_decoder_data decoder_data = {.latest_payload = 0, .written = 0, .alloc = allocator, .latest_error = 0};
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    size_t current_written = 0;
    int err_code = 0;
    while (current_written < first_message_size && !err_code) {
        struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data + current_written, read_size);
        err_code = aws_event_stream_streaming_decoder_pump(&decoder, &test_buf);
        current_written += read_size;
    }

    /* we should have written into the second message, but prior to the new prelude being found.
       check first message was parsed correctly */
    ASSERT_SUCCESS(err_code, "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x00000010, decoder_data.latest_prelude.total_len, "Message length should have been 0x10");
    ASSERT_INT_EQUALS(0x00000000, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0x00");
    ASSERT_INT_EQUALS(0x05c248eb, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0x8c335472");
    ASSERT_INT_EQUALS(0, decoder_data.written, "No payload data should have been written");
    ASSERT_UINT_EQUALS(0x7D98C8FF, decoder_data.message_crc);

    while (current_written < sizeof(test_data) && !err_code) {
        size_t to_write =
            current_written + read_size < sizeof(test_data) ? read_size : sizeof(test_data) - current_written;
        struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data + current_written, to_write);
        err_code = aws_event_stream_streaming_decoder_pump(&decoder, &test_buf);
        current_written += to_write;
    }

    /* Second message should have been found and fully parsed at this point. */
    ASSERT_SUCCESS(err_code, "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(0x0000003D, decoder_data.latest_prelude.total_len, "Message length should have been 0x3D");
    ASSERT_INT_EQUALS(0x00000020, decoder_data.latest_prelude.headers_len, "Headers Length should have been 0x20");
    ASSERT_INT_EQUALS(0x07FD8396, decoder_data.latest_prelude.prelude_crc, "Prelude CRC should have been 0x07FD8396");

    const char *content_type = "content-type";
    const char *content_type_value = "application/json";

    struct aws_event_stream_header_value_pair latest_header;
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 0);
    struct aws_byte_buf latest_header_value = aws_event_stream_header_value_as_string(&latest_header);

    ASSERT_BIN_ARRAYS_EQUALS(
        content_type,
        strlen(content_type),
        latest_header.header_name,
        latest_header.header_name_len,
        "header name should have been %s",
        content_type);
    ASSERT_BIN_ARRAYS_EQUALS(
        content_type_value,
        strlen(content_type_value),
        latest_header_value.buffer,
        latest_header_value.len,
        "header value should have been %s",
        content_type_value);

    const char *expected_str = "{'foo':'bar'}";
    size_t payload_len = decoder_data.latest_prelude.total_len - AWS_EVENT_STREAM_PRELUDE_LENGTH -
                         AWS_EVENT_STREAM_TRAILER_LENGTH - decoder_data.latest_prelude.headers_len;
    ASSERT_INT_EQUALS(
        strlen(expected_str), payload_len, "payload length should have been %d", (int)(strlen(expected_str)));

    ASSERT_BIN_ARRAYS_EQUALS(
        expected_str,
        strlen(expected_str),
        decoder_data.latest_payload,
        payload_len,
        "payload should have been %s",
        expected_str);

    if (decoder_data.latest_payload) {
        aws_mem_release(allocator, decoder_data.latest_payload);
    }
    ASSERT_UINT_EQUALS(0x8D9C08B1, decoder_data.message_crc);

    aws_event_stream_streaming_decoder_clean_up(&decoder);
    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);

    return 0;
}

AWS_TEST_CASE(test_streaming_decoder_incoming_multiple_messages, s_test_streaming_decoder_incoming_multiple_messages_fn)

static int s_test_streaming_decoder_incoming_application_large_size_header_name_valid_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    (void)ctx;

    /* This test replicates the --trigger case from POC which uses:
     * - header name length: 255
     * - header name bytes: 255 (filled with 'A')
     * - header type: 0x00 (bool true)
     * This tests the decoder's handling of large header names (255 bytes) */

    uint8_t name_len = 255;
    size_t name_bytes = 255;
    uint32_t headers_len = (uint32_t)(1 + name_bytes + 1); /* name_len byte + name + type byte */
    uint32_t total_len = (uint32_t)(AWS_EVENT_STREAM_PRELUDE_LENGTH + headers_len + AWS_EVENT_STREAM_TRAILER_LENGTH);

    /* Allocate buffer for the message */
    uint8_t *test_data = aws_mem_acquire(allocator, total_len);
    memset(test_data, 0, total_len);

    /* Write prelude: total_len (4 bytes) */
    test_data[0] = (total_len >> 24) & 0xFF;
    test_data[1] = (total_len >> 16) & 0xFF;
    test_data[2] = (total_len >> 8) & 0xFF;
    test_data[3] = total_len & 0xFF;

    /* Write prelude: headers_len (4 bytes) */
    test_data[4] = (headers_len >> 24) & 0xFF;
    test_data[5] = (headers_len >> 16) & 0xFF;
    test_data[6] = (headers_len >> 8) & 0xFF;
    test_data[7] = headers_len & 0xFF;

    /* Calculate and write prelude CRC */
    uint32_t prelude_crc = aws_checksums_crc32(test_data, 8, 0);
    test_data[8] = (prelude_crc >> 24) & 0xFF;
    test_data[9] = (prelude_crc >> 16) & 0xFF;
    test_data[10] = (prelude_crc >> 8) & 0xFF;
    test_data[11] = prelude_crc & 0xFF;

    /* Write header: name_len */
    test_data[12] = name_len;

    /* Write header: name (filled with 'A') */
    memset(test_data + 13, 'A', name_bytes);

    /* Write header: type (0x07 = string, but we'll use 0x00 = bool true for simplicity) */
    test_data[13 + name_bytes] = 0x00;

    /* Calculate and write message CRC */
    uint32_t message_crc = aws_checksums_crc32(test_data, total_len - 4, 0);
    size_t crc_offset = total_len - 4;
    test_data[crc_offset] = (message_crc >> 24) & 0xFF;
    test_data[crc_offset + 1] = (message_crc >> 16) & 0xFF;
    test_data[crc_offset + 2] = (message_crc >> 8) & 0xFF;
    test_data[crc_offset + 3] = message_crc & 0xFF;

    struct test_decoder_data decoder_data = {
        .latest_payload = 0,
        .written = 0,
        .alloc = allocator,
        .latest_error = 0,
    };
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, total_len);

    ASSERT_SUCCESS(
        aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should have succeeded");
    ASSERT_SUCCESS(decoder_data.latest_error, "No Error callback shouldn't have been called");

    ASSERT_INT_EQUALS(total_len, decoder_data.latest_prelude.total_len, "Message length mismatch");
    ASSERT_INT_EQUALS(headers_len, decoder_data.latest_prelude.headers_len, "Headers length mismatch");
    ASSERT_INT_EQUALS(prelude_crc, decoder_data.latest_prelude.prelude_crc, "Prelude CRC mismatch");

    /* Verify header was parsed correctly */
    ASSERT_TRUE(aws_array_list_length(&decoder_data.headers_list) > 0, "Should have at least one header");
    struct aws_event_stream_header_value_pair latest_header;
    aws_array_list_get_at(&decoder_data.headers_list, &latest_header, 0);
    ASSERT_INT_EQUALS(name_len, latest_header.header_name_len, "Header name length should be 255");

    /* Verify all header name bytes are 'A' */
    for (size_t i = 0; i < name_len; i++) {
        ASSERT_INT_EQUALS('A', latest_header.header_name[i], "Header name byte should be 'A'");
    }

    /* Verify header value is bool true */
    int8_t header_value = aws_event_stream_header_value_as_bool(&latest_header);
    ASSERT_INT_EQUALS(1, header_value, "Header value should be true");

    ASSERT_UINT_EQUALS(message_crc, decoder_data.message_crc, "Message CRC mismatch");

    if (decoder_data.latest_payload) {
        aws_mem_release(allocator, decoder_data.latest_payload);
    }

    aws_event_stream_streaming_decoder_clean_up(&decoder);
    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);
    aws_mem_release(allocator, test_data);

    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_application_large_size_header_name_valid,
    s_test_streaming_decoder_incoming_application_large_size_header_name_valid_fn)

//

/*
00 00 00 14  00 00 00 08  fe 99 66 19   ; prelude, crc=0xFE996619
00 00  00 00  00 00  00 00              ; 4× {name_len=0, type=BOOL_TRUE}
00                                      ; +1 byte to keep pump looping

 */
static int s_test_streaming_decoder_incoming_illegal_header_length_relationship_fails_fn(
    struct aws_allocator *allocator,
    void *ctx) {
    uint8_t test_data[] = {
        0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x08, 0xfe, 0x99, 0x66,
        0x19, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    (void)ctx;
    struct test_decoder_data decoder_data = {.latest_payload = 0, .written = 0, .alloc = allocator, .latest_error = 0};
    aws_event_stream_headers_list_init(&decoder_data.headers_list, allocator);

    struct aws_event_stream_streaming_decoder_options decoder_options = {
        .on_payload_segment = s_decoder_test_on_payload_segment,
        .on_prelude = s_decoder_test_on_prelude_received,
        .on_header = s_decoder_test_header_received,
        .on_complete = s_decoder_test_on_complete,
        .on_error = s_decoder_test_on_error,
        .user_data = &decoder_data};

    struct aws_event_stream_streaming_decoder decoder;
    aws_event_stream_streaming_decoder_init_from_options(&decoder, allocator, &decoder_options);

    struct aws_byte_buf test_buf = aws_byte_buf_from_array(test_data, sizeof(test_data));
    ASSERT_FAILS(aws_event_stream_streaming_decoder_pump(&decoder, &test_buf), "Message validation should not succeed");
    ASSERT_INT_EQUALS(
        AWS_ERROR_EVENT_STREAM_MESSAGE_INVALID_HEADERS_LEN,
        decoder_data.latest_error,
        "Error should be invalid headers length");

    if (decoder_data.latest_payload) {
        aws_mem_release(allocator, decoder_data.latest_payload);
    }

    aws_event_stream_streaming_decoder_clean_up(&decoder);

    aws_event_stream_headers_list_cleanup(&decoder_data.headers_list);

    return 0;
}

AWS_TEST_CASE(
    test_streaming_decoder_incoming_illegal_header_length_relationship_fails,
    s_test_streaming_decoder_incoming_illegal_header_length_relationship_fails_fn)