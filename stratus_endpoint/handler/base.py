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
    def decode( cls, _stat: str ) -> "Status":
        toks = _stat.split(".")
        if len( toks ) > 1:
            assert toks[0].upper() == "STATUS", "Status: attempt to decode a str that is not a status: " + _stat
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

    def __init__( self, rid: str, cid: str, **kwargs ):
        self._rid = rid
        self._cid = cid
        self._status = kwargs.get( "status", Status.IDLE )
        self._parms = kwargs

    @property
    def rid(self):                        # request ID
        return self._rid

    @property
    def cid(self):                        # request ID
        return self._cid

    @abc.abstractmethod
    def getResult(self, timeout=None, block=False) ->  Optional[TaskResult]: pass

    def status(self) ->  Status:
        return self._status

    def __getitem__( self, key: str ) -> str: return self._parms.get( key, None )

    def __str__(self) -> str:
        items = dict( rid=self._rid, cid=self._cid, status=self._status)
        return f"{self.__class__.__name__}{str(items)}"










