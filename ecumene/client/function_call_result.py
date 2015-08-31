from enum import Enum

class FunctionCallResult:
    class Status(Enum):
        Success = 0
        InvalidArgument = 1
        UndefinedReference = 2
        NetworkError = 3
        UnknownError = 4

    def __init__(self, status, result):
        self.status = status
        self.result = result
