from app.config import ENABLE_ALERTS
from app.logger import logger


def send_alert(title: str, message: str) -> None:
    if ENABLE_ALERTS:
        logger.warning(f"ALERT: {title} - {message}")
