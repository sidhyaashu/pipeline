import logging
import sys
from app.config import LOG_LEVEL

def setup_logger(name: str = "pipeline"):
    logger = logging.getLogger(name)
    
    # Avoid duplicate handlers if setup is called multiple times
    if not logger.handlers:
        logger.setLevel(LOG_LEVEL)
        
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(LOG_LEVEL)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        
        logger.addHandler(handler)
        
    return logger

# Create a global logger instance
logger = setup_logger()
