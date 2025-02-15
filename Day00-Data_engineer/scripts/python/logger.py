from colorama import Fore, Style, init
import logging
import os


init(autoreset=True)


LOG_COLORS = {
    "DEBUG": Fore.BLUE,
    "INFO": Fore.GREEN,
    "WARNING": Fore.YELLOW,
    "ERROR": Fore.RED,
    "CRITICAL": Fore.RED + Style.BRIGHT,
}


class ColoredFormatter(logging.Formatter):
    """
    Custom log formatter that applies color coding to log messages.

    This class extends `logging.Formatter`
    to add ANSI color codes to log levels,
    making log output visually distinguishable.
    """

    def format(self, record):
        """
        Formats the log message with an appropriate color.

        Args:
            record (logging.LogRecord):
                The log record containing log information.

        Returns:
            str: The formatted log message with ANSI color codes.
        """

        log_color = LOG_COLORS.get(record.levelname, "")
        log_message = super().format(record)
        return log_color + log_message + Style.RESET_ALL


log_level = os.getenv("PY_LOG_LEVEL", "INFO").upper()
log_level = getattr(logging, log_level, logging.INFO)


console_handler = logging.StreamHandler()
console_handler.setFormatter(
    ColoredFormatter("%(asctime)s - %(levelname)s - %(message)s")
)
console_handler.setLevel(log_level)


logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)
logger.addHandler(console_handler)
