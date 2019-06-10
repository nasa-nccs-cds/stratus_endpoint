from .base import Endpoint, TaskHandle, TaskResult, Status
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple, Optional
import time

class TestEndpoint(Endpoint):

    def __init__( self, **kwargs ):
        Endpoint.__init__( self, **kwargs )
        self._epas = [ f"test{index}" for index in range(10) ]

    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> "TaskHandle":
        workTime = self.getWorktime( requestSpec["operations"] )
        tparms = { **self.parms, **kwargs }
        self.logger.info( f"exec TestEndpoint, request = {requestSpec}, parms = {tparms}")
        return TestTask( workTime, **tparms )

    def getWorktime(self, operations: List[Dict]) -> float :
        return sum( [ float(op.get("workTime", 0.0)) for op in operations ] )

    def shutdown(self, **kwargs ): pass

    def capabilities(self, type: str, **kwargs  ) -> Dict:
        if type == "epas":
            return dict( epas = self._epas )

    def init( self ): pass


class TestTask(TaskHandle):
    BaseTime = None

    def __init__( self, workTime: float, **kwargs ):
        self._parms = kwargs
        TaskHandle.__init__(self, 'tid1', 'rid1', 'cid1', **kwargs )
        self._workTime = workTime
        self._startTime = time.time()
        self._status = Status.EXECUTING
        self._clients = kwargs.get("clients","").split(",")
        self.logger.info( "Starting TestTask[{}:{}] at time {:8.3f}, worktime={:8.2f}, parms = {}, t={:12.3f}".format( self.cid, self.rid, self.elapsed(),workTime,str(kwargs),time.time()))

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
        if self._status == Status.EXECUTING:
            completed = ( time.time() - self._startTime ) > self._workTime
            if completed:
                self.logger.info( f"Completed TestTask at time {self.elapsed()}")
                self._status = Status.COMPLETED
        return self._status


