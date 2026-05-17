import logging
from datetime import datetime
from pathlib import Path

from app.settings.settings import settings


class BaseLogger:
    def __init__(self, name: str):
        self._logger = logging.getLogger(name)
        self._logger.setLevel(logging.INFO)

    def info(self, message: str):
        self._logger.info(message)

    def warning(self, message: str, stack_trace: bool = False):
        self._logger.warning(message, exc_info=stack_trace)

    def error(self, message: str, stack_trace: bool = False):
        self._logger.error(message, exc_info=stack_trace)

    def critical(self, message: str, stack_trace: bool = True):
        self._logger.critical(message, exc_info=stack_trace)


class ConsoleLogger(BaseLogger):
    """
    Logger that outputs to console only.

    Args:
        name: Name to use for the log origin
    """

    def __init__(self, name: str):
        super().__init__(name)

        handler = logging.StreamHandler()
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )

        self._logger.addHandler(handler)


class FileLogger(BaseLogger):
    """
    Logger that outputs to log file only

    Args:
        name: Name to use for the log origin
        path: File path to save logfile to
    """

    def __init__(self, name: str, path: str | None = None):
        super().__init__(name)
        logging_path = Path(path) if path else Path(settings.config.logs.path)
        logging_path = logging_path / str(datetime.now().strftime("%d-%m-%Y"))
        logging_path.mkdir(exist_ok=True, parents=True)
        handler = logging.FileHandler(
            logging_path / f"{datetime.now().strftime('%H:%M:%S %d-%m-%Y')}-{name}.log"
        )
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        self._logger.addHandler(handler)


class UnifiedLogger:
    """
    Logger that outputs to console and logfile at the same time.

    Args:
        name: Name to use for the log origin
        path: File path to save logfile to
    """

    def __init__(self, name: str, path: str | None = None):
        self.__console_logger = ConsoleLogger(name)
        self.__file_logger = FileLogger(name, path)

    def info(self, message: str):
        self.__console_logger.info(message)
        self.__file_logger.info(message)

    def warning(self, message: str, stack_trace: bool = False):
        self.__console_logger.warning(message, stack_trace=stack_trace)
        self.__file_logger.warning(message, stack_trace=stack_trace)

    def error(self, message: str, stack_trace: bool = False):
        self.__console_logger.error(message, stack_trace=stack_trace)
        self.__file_logger.error(message, stack_trace=stack_trace)

    def critical(self, message: str, stack_trace: bool = True):
        self.__console_logger.critical(message, stack_trace=stack_trace)
        self.__file_logger.critical(message, stack_trace=stack_trace)
