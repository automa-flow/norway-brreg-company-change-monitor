"""Run-scoped logging setup."""

from __future__ import annotations

import logging
import sys

from norway_brreg_company_change_monitor.models import ACTOR_NAME


def configure_actor_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="%(asctime)sZ %(levelname)s %(name)s %(message)s",
        force=True,
    )
    # httpx logs the full request URL at INFO, and every BRREG request carries
    # watched organization numbers in the path or the query string. A customer's
    # supplier list does not belong in a run log.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    return logging.getLogger(ACTOR_NAME)
