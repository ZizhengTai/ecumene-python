from io import BytesIO
from concurrent.futures import ThreadPoolExecutor
import types
import msgpack
from time import sleep

from ecumene.client.client_agent import ClientAgent
from ecumene.client.function_call import FunctionCall
from ecumene.client.function_call_result import FunctionCallResult
from ecumene.utils import Future, Promise

class Function:
    def __init__(self, ecm_key, *args):
        self._ecm_key = ecm_key
        self._timeout = 15000
        self._ret_type = args[-1]
        self._arg_types = args[:-1]

    @property
    def ecm_key(self):
        return self._ecm_key

    @ecm_key.setter
    def ecm_key(self, value):
        self._ecm_key = value

    @property
    def timeout(self):
        return self._timeout

    @timeout.setter
    def timeout(self, value):
        self._timeout = value

    def future(self, *args):
        self.__check_args(args)

        packer = msgpack.Packer()
        buf = BytesIO()
        buf.write(packer.pack_array_header(len(args)))
        for arg in args:
            Function.__pack(arg, packer, buf)
        buf.seek(0)
        
        p = Promise()
        def callback(result):
            assert(isinstance(result, FunctionCallResult))

            if result.status == FunctionCallResult.Status.Success:
                p.resolve(Function.__unpack(
                    self._ret_type,
                    msgpack.Unpacker(BytesIO(result.data))))
            elif result.status == FunctionCallResult.Status.InvalidArgument:
                p.reject(TypeError('invalid argument to {0}'.format(self.ecm_key)))
            elif result.status == FunctionCallResult.Status.UndefinedReference:
                p.reject(RuntimeError('undefined reference to {0}'.format(self.ecm_key)))
            elif result.status == FunctionCallResult.Status.NetworkError:
                p.reject(RuntimeError('failed to call {0} due to network error'.format(self.ecm_key)))
            else:
                p.reject(RuntimeError('unknown error when calling {0}'.format(self.ecm_key)))

        ClientAgent.instance().send(FunctionCall(
            ecm_key=self.ecm_key,
            args=buf.read(),
            callback=callback,
            timeout=self.timeout))

        return p.future()

    def __call__(self, *args):
        return self.future(*args).get()

    def __check_args(self, args):
        assert(len(args) == len(self._arg_types))
        for i in range(len(args)):
            assert(isinstance(args[i], self._arg_types[i]))

    @staticmethod
    def __pack(x, packer, buf):
        if isinstance(x, list):
            length = len(x)
            buf.write(packer.pack_array_header(length))
            for elem in x:
                buf.write(packer.pack(elem))
        elif isinstance(x, dict):
            length = len(x)
            buf.write(packer.pack_map_header(length))
            for item in x.items():
                buf.write(packer.pack(item))
        else:
            buf.write(packer.pack(x))

    @staticmethod
    def __unpack(tpe, unpacker):
        if issubclass(tpe, str):
            unpacked = unpacker.unpack().decode('utf-8')
        elif issubclass(tpe, list):
            length = unpacker.read_array_header()
            unpacked = []
            for i in range(length):
                unpacked.append(unpacker.unpack())
            assert(isinstance(unpacked, tpe) and len(unpacked) == length)
        elif issubclass(tpe, dict):
            length = unpacker.read_map_header()
            unpacked = {}
            for i in range(length):
                k = unpacker.unpack()
                v = unpacker.unpack()
                unpacked[k] = v
            assert(isinstance(unpacked, tpe) and len(unpacked) == length)
        else:
            unpacked = unpacker.unpack()
        return tpe(unpacked)
