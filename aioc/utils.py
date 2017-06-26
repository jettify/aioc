import attr
from .state import GossipId



@attr.s
class NodeDescriptor:
    node = attr.ib()
    last_disconnected = attr.ib()
    last_message_id = attr.ib()


@attr.s
class IdGenerator:

    incarnation = attr.ib(default=1)
    sequence_num = attr.ib(default=1)

    def next_incarnation(self) -> int:
        self.incarnation += 1
        return self.incarnation

    def skip_incarnation(self, offset: int) -> int:
        self.incarnation += offset
        return self.incarnation

    def next_sequence_num(self) -> int:
        self.sequence_num += 1
        return self.sequence_num

    def next_message_id(self):
        seq = self.next_sequence_num()
        return GossipId(self.incarnation, seq)
