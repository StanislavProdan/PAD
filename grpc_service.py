"""Integrare gRPC versionata pentru incrementul de mesagerie fiabila.

"""

from __future__ import annotations

from concurrent import futures
from dataclasses import dataclass
import threading
from typing import Callable
from uuid import uuid4

import grpc

import reliable_messaging_pb2 as pb2
import reliable_messaging_pb2_grpc as pb2_grpc


CONTRACT_VERSION = "v1"


@dataclass(frozen=True)
class Operation:
    message_id: str
    outcome: int
    detail: str


class ReliableMessagingServicer(pb2_grpc.ReliableMessagingServicer):
    """Endpoint gRPC idempotent cu fallback explicit pentru statusul operatiei."""

    def __init__(
        self,
        effect: Callable[[str], None] | None = None,
        response_delay: float = 0.0,
    ) -> None:
        self.effect = effect or (lambda payload: None)
        self.response_delay = response_delay
        self._operations: dict[str, Operation] = {}
        self._lock = threading.RLock()

    def Publish(
        self,
        request: pb2.PublishRequest,
        context: grpc.ServicerContext,
    ) -> pb2.PublishResponse:
        self._validate_version(request.contract_version, context)
        if not request.request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id is required")

        with self._lock:
            operation = self._operations.get(request.request_id)
            if operation is None:
                message_id = str(uuid4())
                try:
                    self.effect(request.payload)
                except Exception as error:
                    operation = Operation(
                        message_id=message_id,
                        outcome=pb2.NOT_APPLIED,
                        detail=f"effect failed: {error}",
                    )
                else:
                    operation = Operation(
                        message_id=message_id,
                        outcome=pb2.APPLIED,
                        detail="effect applied",
                    )
                self._operations[request.request_id] = operation

        if self.response_delay:
            if not context.is_active():
                return pb2.PublishResponse(
                    contract_version=CONTRACT_VERSION,
                    request_id=request.request_id,
                    message_id=operation.message_id,
                    outcome=pb2.AMBIGUOUS,
                    detail="operation completed; response deadline expired",
                )
            context.time_remaining()
            threading.Event().wait(self.response_delay)

        return self._publish_response(request.request_id, operation)

    def GetOperationStatus(
        self,
        request: pb2.OperationStatusRequest,
        context: grpc.ServicerContext,
    ) -> pb2.OperationStatusResponse:
        self._validate_version(request.contract_version, context)
        if not request.request_id:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "request_id is required")
        with self._lock:
            operation = self._operations.get(request.request_id)
        if operation is None:
            return pb2.OperationStatusResponse(
                contract_version=CONTRACT_VERSION,
                request_id=request.request_id,
                outcome=pb2.NOT_APPLIED,
                detail="operation not found",
            )
        return pb2.OperationStatusResponse(
            contract_version=CONTRACT_VERSION,
            request_id=request.request_id,
            message_id=operation.message_id,
            outcome=operation.outcome,
            detail=operation.detail,
        )

    @staticmethod
    def _validate_version(version: str, context: grpc.ServicerContext) -> None:
        if version != CONTRACT_VERSION:
            context.abort(
                grpc.StatusCode.UNIMPLEMENTED,
                f"unsupported contract version: {version!r}",
            )

    @staticmethod
    def _publish_response(request_id: str, operation: Operation) -> pb2.PublishResponse:
        return pb2.PublishResponse(
            contract_version=CONTRACT_VERSION,
            request_id=request_id,
            message_id=operation.message_id,
            outcome=operation.outcome,
            detail=operation.detail,
        )


class GrpcMessagingServer:
    """Wrapper pentru ciclul de viata, folosit de teste si aplicatii mici."""

    def __init__(
        self,
        servicer: ReliableMessagingServicer | None = None,
        host: str = "127.0.0.1",
        port: int = 0,
    ) -> None:
        self.servicer = servicer or ReliableMessagingServicer()
        self._server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
        pb2_grpc.add_ReliableMessagingServicer_to_server(self.servicer, self._server)
        self.address = self._server.add_insecure_port(f"{host}:{port}")

    def start(self) -> None:
        self._server.start()

    def stop(self, grace: float = 0.0) -> None:
        self._server.stop(grace).wait()


class GrpcMessagingClient:
    """Client care rezolva ambiguitatea timeout-ului prin verificarea statusului."""

    def __init__(self, target: str, timeout: float = 1.0) -> None:
        self._channel = grpc.insecure_channel(target)
        self._stub = pb2_grpc.ReliableMessagingStub(self._channel)
        self.timeout = timeout

    def close(self) -> None:
        self._channel.close()

    def publish(self, payload: str, request_id: str) -> pb2.PublishResponse:
        request = pb2.PublishRequest(
            contract_version=CONTRACT_VERSION,
            request_id=request_id,
            payload=payload,
        )
        try:
            return self._stub.Publish(request, timeout=self.timeout)
        except grpc.RpcError as error:
            if error.code() not in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.CANCELLED,
            ):
                raise
            return self.resolve_ambiguous(request_id)

    def resolve_ambiguous(self, request_id: str) -> pb2.PublishResponse:
        status = self._stub.GetOperationStatus(
            pb2.OperationStatusRequest(
                contract_version=CONTRACT_VERSION,
                request_id=request_id,
            ),
            timeout=self.timeout,
        )
        return pb2.PublishResponse(
            contract_version=status.contract_version,
            request_id=status.request_id,
            message_id=status.message_id,
            outcome=status.outcome,
            detail=f"fallback status lookup: {status.detail}",
        )
