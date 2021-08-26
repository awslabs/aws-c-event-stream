

/*

class Connection:
    # pretty much the same. or don't use this
    pass


class OperationStream1ReqNResp:

    # users don't create this, the generated model does
    def __init__(self, name_of_request_type, name_of_1st_response_type, name_of_nth_response_type):
        self.name_of_request_type = name_of_request_type
        self.name_of_1st_response_type = name_of_1st_response_type
        self.name_of_nth_response_type = name_of_nth_response_type
        pass

    def _send_request(self, request_payload_as_just_bytes):
        # put together the proper headers
        # send it along
        pass

    def _on_response(self, headers, paylaod_as_bytes):
        # generic ass message came in
        # make sure it's got the right headers
        # do something different if its first vs Nth

        # this function would only be in C
        pass

    def _on_1st_response(self):
        # override me subclass. this would be in language binding

    def _on_nth_response(self):
        # override me subclass

    def _on_shutdown_maybe_we_expected_this_maybe_we_didnt(self):
        pass

    def _close(self):
        pass


class OperationStream1Req1Resp: # SHOULD THIS EVEN BE A CLASS? OR IS IT A FUNCTION?
    pass


def do_1_req_1_response_in_c(name_of_request_type, request_payload_bytes, name_of_response_type, on_complete_callback, callback_userdata):
    # on_complete looks like: on_complete(error_code, headers, payload_as_bytes, name_of_response_type)
    pass


############## generated python greengrass #############


class GeneratedGreengrassClient:
    def publish_to_iot_core(publish_to_iot_core_request) # returns a future that will have an PublishToIotCoreResponse

    def new_subscribe_to_iot_core(self, stream_handler: SubscribeToIoTCoreStreamHandler) -> SubscribeToIoTCoreOperation:
        # looks like it always did
        pass

*/