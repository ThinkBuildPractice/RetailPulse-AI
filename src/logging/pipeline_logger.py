import logging
import os
from src.utils.config_loader import load_config

os.makedirs(
    "logs",
    exist_ok=True
)

config = load_config()

LOG_FILE = config["logging"]["log_file"]

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger(
    "RetailPulse"
)