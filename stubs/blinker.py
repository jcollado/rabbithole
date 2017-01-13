from typing import (
    Optional,
)


class Signal(object):
    def __init__(self) -> None:
        pass

    def connect(self, receiver: object, weak: bool) -> None:
        pass

    def send(self, sender: Optional[object], **kwargs: object) -> None:
        pass
