from stratus_endpoint.handler.base import Status
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Optional
import traceback

class ErrorRecord:

    def __init__(self, message: str, traceback: List[str]):
        self.message = message
        self.traceback = traceback

    def

class RequestMetadata:

    def __init__(self):
        self._messages = []
        self._error: ErrorRecord = None
        self._status = Status.IDLE

    def setError(self, message, traceback=None):
        self.setErrorRecord( ErrorRecord( message, traceback ) )

    def setErrorRecord(self, eRec: ErrorRecord ):
        self._error = eRec
        self._status = Status.ERROR

    def setStatus(self, status: Status ):
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

    def request(self, rid: str ) -> RequestMetadata:
        return self._requestRecs.setdefault( rid, RequestMetadata() )

    def clear( self, rid: str ):
        try: del self._requestRecs[rid]
        except: pass


messageCemter = MessageCenter()