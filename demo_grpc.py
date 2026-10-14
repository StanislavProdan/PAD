from grpc_service import GrpcMessagingClient, GrpcMessagingServer


server = GrpcMessagingServer()
server.start()

client = GrpcMessagingClient(
    f"127.0.0.1:{server.address}",
    timeout=2,
)

try:
    response = client.publish(
        "order-created",
        request_id="order-1",
    )
    print(f"Outcome: {response.outcome}")
    print(f"Message ID: {response.message_id}")
finally:
    client.close()
    server.stop()
