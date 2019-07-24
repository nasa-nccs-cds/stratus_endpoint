from typing import Dict, Any, Union, List, Callable, Optional, Iterable
from stratus_endpoint.util.config import StratusLogger
import zmq, traceback, time, itertools, queue
from stratus_endpoint.handler.base import Status, TaskHandle, TaskResult, Endpoint
from threading import Thread
import abc, string, random, xarray as xa

class Executable:
    __metaclass__ = abc.ABCMeta

    def __init__(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ):
        self.parms = kwargs
        self.logger =  StratusLogger.getLogger()
        self.request = requestSpec
        self.inputs = inputs
        self.requestId = kwargs.get( "rid", self.randomStr(4) )

    @staticmethod
    def randomStr(length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def execute(self, **kwargs) -> TaskResult:
        pass


class SubmissionThread(Thread):

    def __init__(self, job: Executable, processResults, processFailure ):
        Thread.__init__(self)
        self.job = job
        self.processResults = processResults
        self.processFailure = processFailure
        self.logger =  StratusLogger.getLogger()

    def run(self):
        start_time = time.time()
        try:
            self.logger.info( "* Running workflow for requestId " + self.job.requestId)
            results: TaskResult = self.job.execute()
            self.logger.info( "Completed edas workflow in time " + str(time.time()-start_time) )
            self.processResults( results )
        except Exception as err:
            self.logger.error( "Execution error: " + str(err))
            self.logger.error( traceback.format_exc() )
            self.processFailure(err)

class TaskExecHandler(TaskHandle):


    def __init__( self, cid: str, job: Executable, **kwargs ):
        super(TaskExecHandler, self).__init__(**{"rid": job.requestId, "cid": cid, **kwargs})
        self.logger = StratusLogger.getLogger()
        self.sthread = None
        self._processResults = True
        self.results = queue.Queue()
        self.job = job
        self._status = Status.IDLE
        self.start_time = time.time()
        self._exception = None

    def start( self ) -> SubmissionThread:
        self._status = Status.EXECUTING
        self.sthread = SubmissionThread( self.job, self.processResult, self.processFailure )
        self.sthread.start()
        self.logger.info( " ----------------->>> Submitted request for job " + self.job.requestId )
        return self.sthread

    def getResult(self,  **kwargs ) ->  Optional[TaskResult]:
        timeout = kwargs.get('timeout',None)
        block = kwargs.get('block',False)
        return self.results.get( block, timeout )

    def processResult( self, result: TaskResult ):
        self.results.put( result )
        self._status = Status.COMPLETED
        self.logger.info(" ----------------->>> STRATUS REQUEST COMPLETED "  )

    def status(self):
        return self._status

    @classmethod
    def getTbStr( cls, ex ) -> str:
        if ex.__traceback__  is None: return ""
        tb = traceback.extract_tb( ex.__traceback__ )
        return " ".join( traceback.format_list( tb ) )

    @classmethod
    def getErrorReport( cls, ex ):
        try:
            errMsg = getattr( ex, 'message', repr(ex) )
            return errMsg + ">~>" +  str( cls.getTbStr(ex) )
        except:
            return repr(ex)

    def processFailure(self, ex: Exception):
        error_message = self.getErrorReport( ex )
        self.logger.error( error_message )
        self._status = Status.ERROR
        self._exception = ex

    def exception(self) -> Optional[Exception]:
        return self._exception


class ExecEndpoint(Endpoint):
    __metaclass__ = abc.ABCMeta

    def __init__(self, **kwargs ):
        Endpoint.__init__( self, **kwargs )
        self.handlers: Dict[str,TaskHandle] = {}

    def request(self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> TaskHandle:
        rid: str = kwargs.get('rid', Executable.randomStr(4))
        cid: str = kwargs.get('cid', Executable.randomStr(4))
        self.logger.info(f"EDAS Endpoint--> processing rid {rid}")
        executable = self.createExecutable( requestSpec, inputs, **kwargs )
        handler = TaskExecHandler( cid, executable, **kwargs )
        handler.start()
        self.handlers[rid] = handler
        return handler

    def getHandler( self, rid: str ) -> Optional[TaskHandle]:
        return self.handlers.get(rid)

    @abc.abstractmethod
    def createExecutable( self, requestSpec: Dict, inputs: List[TaskResult] = None, **kwargs ) -> Executable: pass