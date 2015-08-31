import time, zmq

class FunctionCall:
    def __init__(self, ecm_key, args, callback, timeout):
        self._ecm_key = ecm_key
        self._args = args
        self._callback = callback
        self._timeoutAt = time.time() + timeout

    @property
    def ecm_key(self):
        return self._ecm_key

    @property
    def args(self):
        return self._args

    @property
    def callback(self):
        return self._callback

    @property
    def timeoutAt(self):
        return self._timeoutAt
