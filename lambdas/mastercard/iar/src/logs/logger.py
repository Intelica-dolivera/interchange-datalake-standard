import logging
import os
import sys

LOG_LEVEL = os.environ.get("ITX_LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ],
    force=True
)

logger = logging.getLogger("pipeline_iar")

class Logger:
    def __init__(self, name: str):
        self.logger = logging.getLogger(name)