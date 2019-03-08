import json, string, random, abc, os
from enum import Enum, auto
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa

class Status(Enum):
    IDLE = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    ERROR = auto()
    UNKNOWN = auto()

    @classmethod
    def decode( cls, status: str ) -> "Status":
        toks = status.split(".")
        if len( toks ) > 1:
            assert toks[0].upper() == "STATUS", "Status: attempt to decode a str that is not a status: " + status
            stat = toks[1].upper()
        else:
            stat = toks[0].upper()
        if stat == "IDLE":          return cls.IDLE
        elif stat == "EXECUTING":   return cls.EXECUTING
        elif stat == "COMPLETED":   return cls.COMPLETED
        elif stat == "ERROR":       return cls.ERROR
        elif stat == "UNKNOWN":     return cls.UNKNOWN
        raise Exception( "Unrecognized status: " + stat )

class Endpoint:
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        pass

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def request(self, requestSpec: Dict, **kwargs ) -> "Task": pass

    @abc.abstractmethod
    def shutdown(self, **kwargs ): pass

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def init( self ): pass

class TaskResult:
    def __init__(self, header: Dict, data: List[xa.Dataset] = None ):
        self.header = header
        self.data = [] if data is None else data

    @property
    def type(self) -> str:
        return self.header.get( "type", "")

class Task:
    __metaclass__ = abc.ABCMeta

    def __init__( self, sid: str, **kwargs ):
        self._sid = sid                                      # submission ID
        self._status = kwargs.get( "status", Status.IDLE )
        self._parms = kwargs

    @property
    def id(self):                        # submission ID
        return self._sid

    @property
    def sid(self):                        # submission ID
        return self._sid

    @property
    def cid(self):                        # client ID
        return self._sid.split("-")[0]

    @property
    def rid(self):                        # request ID
        return self._sid.split("-")[1]

    @abc.abstractmethod
    def getResult(self, timeout=None, block=False) ->  Optional[TaskResult]: pass

    def status(self) ->  Status:
        return self._status

    def __getitem__( self, key: str ) -> str: return self._parms.get( key, None )


class TestTask(Task):

    def __init__(self, type: str, request: Dict, **kwargs):
        Task. __init__(self, request['sid'], **kwargs)
        self.request = request
        self.type = type

    def getResult(self, timeout=None, block=False) -> Optional[TaskResult]:
        header = dict(self.request)
        return TaskResult(header)










