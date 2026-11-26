#!/usr/bin/env python3
"""Top-level convenience entry: ``python3 service.py serve main-local.db``."""

from ledger_service import Ledger, LedgerError
from ledger_service.cli import main
from ledger_service.server import create_server, serve

__all__ = ["Ledger", "LedgerError", "create_server", "serve", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
