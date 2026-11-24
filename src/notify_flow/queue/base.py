from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class QueueMessage:
    key: str
    value: bytes
    headers: dict[str, str]


Handler = Callable[[QueueMessage], Awaitable[None]]


class EventPublisher(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def publish(self, key: str, value: bytes, headers: dict[str, str]) -> None: ...


class EventConsumer(Protocol):
    async def run(self, handler: Handler) -> None:
        """Deliver each message to handler; acknowledge only after handler returns.
        Delivery is at-least-once, so handlers must be idempotent."""

    async def stop(self) -> None: ...
