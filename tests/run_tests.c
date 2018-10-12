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
#include <message_deserializer_test.c>
#include <message_serializer_test.c>
#include <message_streaming_decoder_test.c>

int main(int argc, char *argv[]) {

    AWS_RUN_TEST_CASES(&test_incoming_no_op_valid_test,
                       &test_incoming_application_data_no_headers_valid_test,
                       &test_incoming_application_one_compressed_header_pair_valid_test,
                       &test_incoming_application_int32_header_valid_test,

                       &test_outgoing_no_op_valid_test,
                       &test_outgoing_application_data_no_headers_valid_test,
                       &test_outgoing_application_one_compressed_header_pair_valid_test,

                       &test_streaming_decoder_incoming_no_op_valid_single_message_test,
                       &test_streaming_decoder_incoming_application_no_headers_test,
                       &test_streaming_decoder_incoming_application_one_compressed_header_pair_valid_test,
                       &test_streaming_decoder_incoming_multiple_messages_test
    );

}
