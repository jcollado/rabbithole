from typing import (  # noqa
    Dict,
    List,
    Optional,
)


class Signal(object):
    def __init__(self):
        # type: () -> None
        pass

    def connect(self, receiver, weak):
        # type: (object, bool) -> None
        pass

    def send(self, sender, **kwargs):
        # type: (Optional[object], List[Dict[str, object]]) -> None
        pass
