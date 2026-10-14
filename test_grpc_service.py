import unittest

import grpc

from grpc_service import (
    CONTRACT_VERSION,
    GrpcMessagingClient,
    GrpcMessagingServer,
    ReliableMessagingServicer,
)
import reliable_messaging_pb2 as pb2
import reliable_messaging_pb2_grpc as pb2_grpc


class GrpcMessagingTests(unittest.TestCase):
    def setUp(self):
        self.effects = []
        self.server = GrpcMessagingServer(
            ReliableMessagingServicer(self.effects.append)
        )
        self.server.start()
        self.client = GrpcMessagingClient(f"127.0.0.1:{self.server.address}")

    def tearDown(self):
        self.client.close()
        self.server.stop()

    def test_publish_is_versioned_and_idempotent(self):
        first = self.client.publish("created", "request-1")
        second = self.client.publish("created", "request-1")

        self.assertEqual(first.contract_version, CONTRACT_VERSION)
        self.assertEqual(first.outcome, pb2.APPLIED)
        self.assertEqual(second.message_id, first.message_id)
        self.assertEqual(self.effects, ["created"])

    def test_deadline_uses_status_fallback_after_effect(self):
        self.client.close()
        delayed_server = GrpcMessagingServer(
            ReliableMessagingServicer(self.effects.append, response_delay=0.15)
        )
        delayed_server.start()
        delayed_client = GrpcMessagingClient(
            f"127.0.0.1:{delayed_server.address}", timeout=0.03
        )
        try:
            response = delayed_client.publish("once", "request-timeout")
            self.assertEqual(response.outcome, pb2.APPLIED)
            self.assertIn("fallback status lookup", response.detail)
            self.assertEqual(self.effects, ["once"])
        finally:
            delayed_client.close()
            delayed_server.stop()

    def test_unsupported_contract_version_is_rejected(self):
        channel = grpc.insecure_channel(f"127.0.0.1:{self.server.address}")
        stub = pb2_grpc.ReliableMessagingStub(channel)
        with self.assertRaises(grpc.RpcError) as raised:
            stub.Publish(
                pb2.PublishRequest(
                    contract_version="v2",
                    request_id="request-version",
                    payload="not-applied",
                ),
                timeout=1,
            )
        self.assertEqual(raised.exception.code(), grpc.StatusCode.UNIMPLEMENTED)
        channel.close()


if __name__ == "__main__":
    unittest.main()
