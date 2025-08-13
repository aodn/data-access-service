
class ValueCount:

    def __init__(self, value: any, count: int):
        self.value = value
        self.count = count

    def __eq__(self, other):
        if not isinstance(other, ValueCount):
            return NotImplemented
        return self.value == other.value and self.count == other.count

    def __hash__(self):
        return hash((self.value, self.count))