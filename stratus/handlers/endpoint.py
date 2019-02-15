import json, string, random, abc, os
from typing import List, Dict, Any, Sequence, BinaryIO, TextIO, ValuesView, Tuple

class Endpoint:
    __metaclass__ = abc.ABCMeta

    def __init__( self, **kwargs ):
        pass

    @classmethod
    def randomStr(cls, length) -> str:
        tokens = string.ascii_uppercase + string.ascii_lowercase + string.digits
        return ''.join(random.SystemRandom().choice(tokens) for _ in range(length))

    @abc.abstractmethod
    def request(self, type: str, **kwargs ) -> Dict: pass

    @abc.abstractmethod
    def epas( self ) -> List[str]: pass

    @abc.abstractmethod
    def init( self ): pass

