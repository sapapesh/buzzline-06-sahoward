# utils/utils_logger.py

import logging
import os
from datetime import datetime

# Create logs directory if it doesn't exist
LOG_DIR = os.path.join(os.getcwd(), "logs")
os.makedirs(LOG_DIR, exist_ok=True)

# Create a timestamped log filename
LOG_FILE = os.path.join(LOG_DIR, f"producer_{datetime.now().strftime('%Y%m%d')}.log")

# Configure logging format
LOG_FORMAT = (
    "%(asctime)s — %(levelname)s — %(name)s — %(message)s"
)

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # can change to DEBUG for more details
    format=LOG_FORMAT,
    handlers=[
        logging.StreamHandler(),          # Output to console
        logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")  # Save to file
    ]
)

# Create a logger instance
logger = logging.getLogger("NetflixProducer")
