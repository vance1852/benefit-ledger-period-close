"""可复用的跨机构服务券账本服务。

 only standard library is required. 对外稳定入口建议使用
 :class:`ledger_service.ledger.Ledger` 或 HTTP API。
"""

from .errors import LedgerError
from .ledger import Ledger

__all__ = ["Ledger", "LedgerError"]
__version__ = "1.0.0"
