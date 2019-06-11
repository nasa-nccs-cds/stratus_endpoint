from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
from stratus_endpoint.util.config import StratusLogger
import traceback
from enum import Enum, auto

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
        sstr = str(_stat).lower()
        try: return sstr.split(".")[1]
        except: return sstr

class ErrorRecord:

    def __init__(self, message: str, traceback: List[str]):
        self.message = message
        self.traceback = traceback

    def str(self):
        return f"Execption[ {self.message} ]:\n" + "\n".join( self.traceback )

    def repr(self):
        return f"EX:{self.message}$" + "$".join(self.traceback)

class RequestMetadata:

    def __init__( self, ID: str ):
        self.logger = StratusLogger.getLogger()
        self._messages = []
        self._error: ErrorRecord = None
        self._status = Status.IDLE
        self._id = ID

    def setError(self, message, traceback=None):
        self.setErrorRecord( ErrorRecord( message, traceback ) )

    def setErrorRecord(self, eRec: ErrorRecord ):
        self._error = eRec
        self._status = Status.ERROR

    def setStatus(self, status: Status ):
        self.logger.info( f"RequestMetadata[{self._id}] Set Status: {status} ")
        self._status = status

    def setException(self, ex: Exception ):
        self.setError(  getattr(ex, 'message', repr(ex)), traceback.format_tb(ex.__traceback__) )

    def addMessage( self, message ):
        self._messages.append( message )

    @property
    def status(self) -> Status:
        return self._status

    @property
    def error(self) -> Optional[ErrorRecord]:
        return self._error

    @property
    def messages(self) -> List[str]:
        return self._messages

class MessageCenter:

    def __init__(self):
        self._requestRecs = {}

    def request(self, ID: str ) -> RequestMetadata:
        return self._requestRecs.setdefault( ID, RequestMetadata(ID) )

    def clear( self, rid: str ):
        try: del self._requestRecs[rid]
        except: pass


messageCenter = MessageCenter()