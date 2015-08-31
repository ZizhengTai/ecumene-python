from threading import Event, Thread
 
class Promise:
    def __init__(self):
        self._event = Event()
        self._rejected = False
        self._result = None
        self._retrieved = False
 
    def resolve(self, value):
        self._rejected = False
        self._result = value
        self._event.set()
 
    def reject(self, reason):
        self._rejected = True
        self._result = reason
        self._event.set()
 
    def future(self):
        if self._retrieved:
            raise RuntimeError('future already retrieved')
        self._retrieved = True
        future = Future(self)
        return future
 

class Future:
    def __init__(self, promise):
        self._promise = promise

    def get(self):
        self.wait()
        if self._promise._rejected:
            raise self._promise._result
        else:
            return self._promise._result
 
    def then(self, resolved=None, rejected=None):
        promise = Promise()
 
        def task():
            try:
                self._promise._event.wait()
                if self._promise._rejected:
                    result = self._promise._result
                    if rejected:
                        result = rejected(self._promise._result)
                        
                    promise.reject(result)
                else:
                    result = self._promise._result
 
                    if resolved:
                        result = resolved(self._promise._result)
 
                    promise.resolve(result)
            except Exception as e:
                promise.reject(e.message)
 
        Thread(target=task).start()
 
        return promise.future()
 
    def wait(self):
        self._promise._event.wait()
 
    @staticmethod
    def wait_all(*args):
        for future in args:
            future.wait()
