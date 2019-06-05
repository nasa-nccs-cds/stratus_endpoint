import json, string, random, abc, time
from enum import Enum, auto
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional, Iterable
from stratus_endpoint.util.config import StratusLogger
import xarray as xa
from concurrent.futures import Future

class Status(Enum):
    IDLE = auto()
    EXECUTING = auto()
    COMPLETED = auto()
    CANCELED = auto()
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
        elif stat == "CANCELED":    return cls.CANCELED
        elif stat == "ERROR":       return cls.ERROR
        elif stat == "UNKNOWN":     return cls.UNKNOWN
        raise Exception( "Unrecognized status: " + stat )

    @classmethod
    def str( cls, _stat: "Status" ) -> str:
        return str(_stat).lower().split(".")[1]


class TaskResult:
    def __init__(self, header: Dict, data: List[xa.Dataset] = None ):
        self.header = header
        self.data: List[xa.Dataset] = [] if data is None else data

    def getDataset(self) -> xa.Dataset:
        return xa.merge( self.data )

    def getResultClass(self):
        return self.header.get( "@ResultClass", "DATA" )

    def getResultType(self):
        return self.header.get( "@ResultType", "DATA" )

    def popDataset(self) -> Optional[xa.Dataset]:
        if self.empty(): return None
        return self.data.pop(0)

    def empty(self) -> bool:
        return self.data is None or len(self.data) == 0

    def size(self) -> int:
        return 0 if self.data is None else len(self.data)

    def __len__(self):
        return 0 if self.data is None else len(self.data)

    def __str__(self):
        return f"TR{str({**self.header,'size':self.size()})}"

    @property
    def type(self) -> str:
        return self.header.get( "type", "")

    @staticmethod
    def merge( results: List["TaskResult"] ) -> Optional["TaskResult"]:
        if len(results) == 0:      return None
        elif len( results ) == 1:  return results[0]
        else:
            header = {}
            data: List[xa.Dataset] = []
            for index, result in enumerate(results):
                for key, value in result.header.items():
                    header[f"{key}~{index}"] = value
                data.extend( result.data )
        return TaskResult( header, data )


class TaskHandle:
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        self.logger = StratusLogger.getLogger()
        self._parms = kwargs
        self._rid = self.getAssignParm("rid")
        self._cid = self.getAssignParm("cid")
        self._clients = kwargs.get("clients","").split(",")
        self._pollPeriod = float( kwargs.get("pollPeriod", 1.0) )

    @property
    def rid(self):                        # request ID
        return self._rid

    @property
    def cid(self):                        # request ID
        return self._cid

    def getAssignParm(self, key: str, plen = 6 ) -> str :
        return self._parms.get( key, Endpoint.randomStr(6) )

    def hasClient(self, cid: str ) -> bool:
        return cid in self._clients

    @abc.abstractmethod
    def getResult( self, **kwargs ) ->  Optional[TaskResult]: pass

    def waitUntilReady( self ):
        status = Status.IDLE
        while status == Status.IDLE or status == Status.EXECUTING:
            time.sleep( self._pollPeriod )
            status = self.status()

    def blockForResult( self, **kwargs ) ->  Optional[TaskResult]:
        while True:
            if self.status() == Status.ERROR: return None
            result = self.getResult( **kwargs )
            if result is not None: return result
            time.sleep( 0.2 )

    @abc.abstractmethod
    def status(self) ->  Status: pass

    @abc.abstractmethod
    def exception(self) -> Optional[Exception]: pass

    def __getitem__( self, key: str ) -> str: return self._parms.get( key, None )

    def __str__(self) -> str:
        items = dict( rid=self._rid, cid=self._cid, status=self.status())
        return f"{self.__class__.__name__}{str(items)}"

class TaskFuture(TaskHandle):

    def __init__( self, future: Future, **kwargs ):
        TaskHandle.__init__(self, **kwargs)
        self._future = future

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        return self._future.result() if self._future.done() else None

    def cancel(self):
        self._future.cancel()

    def exception(self) -> Optional[Exception]:
        return self._future.exception()

    def status(self) ->  Status:
        if self._future.done():
            if self._future.exception() is not None:  return Status.ERROR
            elif self._future.cancelled():            return Status.CANCELED
            else:                                     return Status.COMPLETED
        else: return Status.EXECUTING

class FailedTask(TaskHandle):

    def __init__( self, exception: Exception, **kwargs ):
        TaskHandle.__init__(self, **kwargs)
        self.logger.info( f"FailedTask: exception = {exception}")
        self._exception = exception

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        return  None

    def cancel(self):
        pass

    def exception(self) -> Optional[Exception]:
        return self._exception

    def status(self) ->  Status:
        return Status.ERROR

class Endpoint:
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        self.parms = kwargs
        self.logger = StratusLogger.getLogger()

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> "TaskHandle": pass

    @abc.abstractmethod
    def shutdown(self, **kwargs ): pass

    @abc.abstractmethod
    def capabilities(self, type: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def init( self ): pass










