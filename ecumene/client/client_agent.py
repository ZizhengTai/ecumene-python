import zmq
import signal, struct, sys, threading

from ecumene.client.function_call_result import FunctionCallResult

class ClientAgent:
    PROTOCOL_VERSION = 0

    _instance = None

    @classmethod
    def instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self._sequence = 0
        self._calls = {}
        self._calls_lock = threading.Lock()
        self._actor = threading.Thread(target=self._actor_task)
        self._actor_pipe = zmq.Context.instance().socket(zmq.PAIR)
        self._actor_pipe.bind('inproc://client_agent')

        def signal_handler(sig, frame):
            self.term()
            sys.exit(0)
        signal.signal(signal.SIGINT, signal_handler)

        self._actor.start()

    def send(self, call):
        self._calls_lock.acquire()
        try:
            self._calls[self._sequence] = call
            self._actor_pipe.send_string('$SEND', flags=zmq.SNDMORE)
            self._actor_pipe.send_string(str(self._sequence))
            self._sequence += 1
        finally:
            self._calls_lock.release()

    def term(self):
        self._actor_pipe.send_string('$TERM', flags=zmq.NOBLOCK)
        self._actor.join()

    def _actor_task(self):
        ctx = zmq.Context.instance()

        pipe = ctx.socket(zmq.PAIR)
        pipe.connect('inproc://client_agent')

        ecm = ctx.socket(zmq.DEALER)
        ecm.connect('tcp://ecumene.io:23332')

        workers = {}

        poller = zmq.Poller()
        poller.register(pipe, flags=zmq.POLLIN)
        poller.register(ecm, flags=zmq.POLLIN)

        terminated = False
        while not terminated:
            events = dict(poller.poll())

            if pipe in events:
                msg = pipe.recv_multipart()
                cmd = msg.pop(0)
                if cmd == b'$TERM':
                    terminated = True
                elif cmd == b'$SEND':
                    local_id = int(msg[0])

                    self._calls_lock.acquire()
                    try:
                        if local_id in self._calls:
                            call = self._calls[local_id]

                            if call.ecm_key in workers:
                                # Use existing socket

                                worker = workers[call.ecm_key]
                                worker.send_string(str(local_id), flags=zmq.SNDMORE)
                                worker.send(call.args)
                            else:
                                # Ask Ecumene for new worker

                                ecm.send(struct.pack('H', ClientAgent.PROTOCOL_VERSION),
                                         flags=zmq.SNDMORE)
                                ecm.send_string(str(local_id), flags=zmq.SNDMORE)
                                ecm.send_string(call.ecm_key)
                    finally:
                        self._calls_lock.release()
            elif ecm in events:
                # Worker assignment from Ecumene
                
                print('Assigned!')

                msg = ecm.recv_multipart()
                if (len(msg) == 4):
                    local_id = int(msg.pop(0).decode('utf-8'))
                    ecm_key = msg.pop(0).decode('utf-8')
                    status = msg.pop(0)
                    endpoint = msg.pop(0).decode('utf-8')

                    if status == b'':
                        # Success

                        if ecm_key not in workers:
                            worker = ctx.socket(zmq.DEALER)
                            worker.connect(endpoint)
                            workers[ecm_key] = worker

                            poller.register(worker, flags=zmq.POLLIN)

                            self._calls_lock.acquire()
                            try:
                                self._actor_pipe.send_string('$SEND', flags=zmq.SNDMORE)
                                self._actor_pipe.send_string(str(local_id))
                            finally:
                                self._calls_lock.release()
                    elif status == b'U':
                        # Undefined reference

                        self._calls_lock.acquire()
                        try:
                            if local_id in self._calls:
                                call = self._calls[local_id]
                                del self._calls[local_id]
                                call.callback(FunctionCallResult(
                                    status=b'U',
                                    result=b''))
                        finally:
                            self._calls_lock.release()
            else:
                # Response from some worker

                for worker in workers.values():
                    if worker in events:
                        msg = worker.recv_multipart()
                        if (len(msg) == 3):
                            local_id = int(msg.pop(0))
                            status = msg.pop(0)
                            result = msg.pop(0)

                            self._calls_lock.acquire()
                            try:
                                if local_id in self._calls:
                                    call = self._calls[local_id]
                                    del self._calls[local_id]
                                    call.callback(FunctionCallResult(
                                        status=status,
                                        result=result))
                            finally:
                                self._calls_lock.release()

        ecm.close()
        pipe.close()
        print('Cleaned up client agent.')
