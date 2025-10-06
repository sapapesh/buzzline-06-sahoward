import logging

# Configure logger
logger = logging.getLogger("NetflixProducer")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s — %(levelname)s — %(name)s — %(message)s"
)

ch = logging.StreamHandler()
ch.setFormatter(formatter)
logger.addHandler(ch)
