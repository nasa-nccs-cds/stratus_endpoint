from .base import Endpoint, TaskHandle, TaskResult, Status
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import time

class TestEndpoint(Endpoint):

    def __init__( self, **kwargs ):
        Endpoint.__init__( self, **kwargs )
        self._epas = [ f"test{index}" for index in range(10) ]

    def request(self, requestSpec: Dict, **kwargs ) -> "TaskHandle":
        workTime = float( requestSpec.get( "workTime", 0.0 ) )
        print( f"exec TestEndpoint, request = {requestSpec}")
        return TestTask( workTime )

    def shutdown(self, **kwargs ): pass

    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )

    def init( self ): pass


class TestTask(TaskHandle):
    BaseTime = None

    def __init__( self, workTime: float, **kwargs ):
        TaskHandle.__init__(self, Endpoint.randomStr(6), Endpoint.randomStr(6))
        self._workTime = workTime
        self._startTime = time.time()
        self._parms = kwargs
        self._clients = kwargs.get("clients","").split(",")
        print( f"Starting TestTask at time {self.elapsed()}, parms = {kwargs}")

    def elapsed(self):
        if TestTask.BaseTime is None: TestTask.BaseTime = time.time()
        return time.time() - TestTask.BaseTime

    @property
    def rid(self):
        return self._rid

    @property
    def cid(self):
        return self._cid

    def hasClient(self, cid: str ) -> bool:
        return cid in self._clients

    def getResult( self, **kwargs ) ->  Optional[TaskResult]:
        return TaskResult( dict( type="test" ) ) if self.status() == Status.COMPLETED else None

    def status(self) ->  Status:
        completed = ( time.time() - self._startTime ) > self._workTime
        if completed: print( f"Completed TestTask at time {self.elapsed()}")
        return Status.COMPLETED if completed else Status.EXECUTING


