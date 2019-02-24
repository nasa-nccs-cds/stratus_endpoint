import json, string, random, abc, os
from enum import Enum, auto
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import xarray as xa

class Status(Enum):
    IDLE = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    ERROR = auto()

class Endpoint:
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        pass

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def request(self, type: str, **kwargs ) -> "Task": pass

    @abc.abstractmethod
    def shutdown(self, **kwargs ): pass

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def init( self ): pass

class Task:
    __metaclass__ = abc.ABCMeta

    def __init__( self, taskID: str, **kwargs ):
        self._taskID = taskID
        self._status = kwargs.get( "status", Status.IDLE )
        self._parms = kwargs

    @property
    def id(self):
        return self._taskID

    @abc.abstractmethod
    def getResult(self, timeout=None, block=False) ->  Optional[xa.Dataset]: pass

    @property
    def status(self) ->  Status:
        return self._status

    def __getitem__( self, key: str ) -> str: return self._parms.get( key, None )

