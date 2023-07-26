from loguru import logger
from settings import LOG_CONFIG


logger.add(**LOG_CONFIG)
