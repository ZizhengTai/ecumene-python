from enum import Enum

class FunctionCallResult:
    class Status(Enum):
        Success = 0
        InvalidArgument = 1
        UndefinedReference = 2
        NetworkError = 3
        UnknownError = 4

    def __init__(self, status, result):
        self.data = result
        if status == b'':
            self.status = FunctionCallResult.Status.Success
        elif status == b'I':
            self.status = FunctionCallResult.Status.InvalidArgument
        elif status == b'U':
            self.status = FunctionCallResult.Status.UndefinedReference
        elif status == b'N':
            self.status = FunctionCallResult.Status.NetworkError
        else:
            self.status = FunctionCallResult.Status.UnknownError
