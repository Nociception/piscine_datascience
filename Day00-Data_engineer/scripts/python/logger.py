from colorama import Fore, Style, init
import logging, os


init(autoreset=True)


LOG_COLORS = {
    "DEBUG": Fore.BLUE,
    "INFO": Fore.GREEN,
    "WARNING": Fore.YELLOW,
    "ERROR": Fore.RED,
    "CRITICAL": Fore.RED + Style.BRIGHT,
}

class ColoredFormatter(logging.Formatter):
    """DOCSTRING"""
    def format(self, record):
        log_color = LOG_COLORS.get(record.levelname, "")
        log_message = super().format(record)
        return log_color + log_message + Style.RESET_ALL


log_level = os.getenv("PY_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level, logging.INFO)


console_handler = logging.StreamHandler()
console_handler.setFormatter(ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s"))
console_handler.setLevel(log_level)  


logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)  
logger.addHandler(console_handler)


logger.debug("log DEBUG.")
logger.info("log INFO.")
logger.warning("log WARNING.")
logger.error("log ERROR.")
logger.critical("log CRITICAL.")
