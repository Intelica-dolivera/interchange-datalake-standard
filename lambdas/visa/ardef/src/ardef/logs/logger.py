import logging
import logging.handlers
import os
from collections import OrderedDict

# Solo carga .env si estamos en local (no en Lamda)
running_in_lambda = "AWS_LAMBDA_FUNCTION_NAME" in os.environ
if not running_in_lambda:
    import dotenv
    dotenv.load_dotenv()


class Logger:
    """
    Provides a standardized logger object to print and store log messages.

    En Lambda: solo StreamHandler -> stdout -> CloudWatch Logs automáticamente.
    En local: StreamHandler -> FileHandler -> consola -> archivo de log en disco.
    """

    _LOG_LEVELS = OrderedDict(
        {
            "debug": logging.DEBUG,
            "info": logging.INFO,
            "warning": logging.WARNING,
            "error": logging.ERROR,
            "critical": logging.CRITICAL,
        }
    )

    _DEFAULT_FMT = (
        "%(asctime)s :: PID %(process)d :: TID %(thread)d :: "
        "%(module)s.%(funcName)s :: Line %(lineno)d :: "
        "%(levelname)s :: %(message)s"
    )

    def __init__(self, name: str) -> None:

        self.logger = logging.getLogger(name)

        if self.logger.handlers:
            return

        log_level = os.environ.get("ITX_LOG_LEVEL", "info")
        self.logger.setLevel(self._LOG_LEVELS[log_level])

        formatter = logging.Formatter(self._DEFAULT_FMT)

        # StreamHandler siempre activo
        # En Lambda, stdout es capturado automáticamente por CloudWatch.
        # En local, imprime en consola.
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        #FileHandler solo en local (cuando NO estamos en Lambda).
        # Lambda inyecta AWS_LAMBDA_FUNCTION_NAME automáticamente.
        # Si esa variable no existe -> entorno local -> agregar FileHandler.
        running_in_lambda = "AWS_LAMBDA_FUNCTION_NAME" in os.environ

        if not running_in_lambda:
            log_path = os.environ.get("ITX_LOG_PATH", "ardef/logs/ardef.log")
            file_handler = logging.handlers.TimedRotatingFileHandler(
                filename=log_path,
                when='D',
                backupCount=3,
                encoding='utf-8',
                delay=True,
            )
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
