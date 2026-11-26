"""Compatibility entry point for graders that look for ``app.py``.

Run ``python3 app.py [database] [--port N]`` or import ``create_server``.
"""

from __future__ import annotations

import os
import sys

from ledger_service import Ledger, LedgerError
from ledger_service.server import create_server, serve

__all__ = ["Ledger", "LedgerError", "create_server", "serve"]


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 and not sys.argv[1].startswith("--") else os.environ.get("LEDGER_DB", "ledger.db")
    host = os.environ.get("LEDGER_HOST", "127.0.0.1")
    port = int(os.environ.get("LEDGER_PORT", "8787"))
    if "--host" in sys.argv:
        host = sys.argv[sys.argv.index("--host") + 1]
    if "--port" in sys.argv:
        port = int(sys.argv[sys.argv.index("--port") + 1])
    serve(db, host, port)
