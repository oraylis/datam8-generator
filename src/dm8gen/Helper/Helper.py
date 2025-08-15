import hashlib
import json
import logging
import os
import shutil
import textwrap
from pathlib import Path
from uuid import UUID

from jsonschema import validate


class Helper:
    """Utility class for common helper functions."""

    @staticmethod
    def validate_json_schema(path_json: str, path_json_schema: str):
        """Validate a JSON file against a JSON schema.

        Args:
            path_json (str): Path to the JSON file.
            path_json_schema (str): Path to the JSON schema file.
        """
        with open(path_json, encoding="utf-8") as f:
            js = json.load(f)

        # Read schema
        with open(path_json_schema, encoding="utf-8-sig", errors="ignore") as f:
            js_schema = json.load(f, strict=False)

        validate(js, schema=js_schema)

    @staticmethod
    def read_json(path: str):
        """Read JSON data from a file.

        Args:
            path (str): Path to the JSON file.

        Returns:
            dict: JSON data.
        """
        with open(path, encoding="utf-8", mode="r") as f:
            try:
                __json = json.load(f)
            except Exception as e:
                raise JsonFileParseException(e, path)

        return __json

    @staticmethod
    def coalesce(values: list):
        """
        Coalesce implementation to get first None value of a list
        """
        return next((v for v in values if v is not None), None)

    @staticmethod
    def get_locator(
        entity_type: str, data_product: str, data_module: str, entity_name: str
    ) -> str:
        """Get locator string for an entity.

        Args:
            entity_type (str): Type of the entity.
            data_product (str): Data product name.
            data_module (str): Data module name.
            entity_name (str): Entity name.

        Returns:
            str: Locator string.
        """
        locator = f"/{entity_type}/{data_product}/{data_module}/{entity_name}"
        return locator

    @staticmethod
    def start_logger(
        log_name: str = "template log",
        log_directory: str = f"{Path(__file__).parents[1]}\\Logs",
        enable_write_log: bool = False,
        log_level: logging.log = logging.INFO,
    ) -> logging.Logger:
        """Initialize and configure a logger.

        Args:
            log_name (str, optional): Name of the logger. Defaults to "template log".
            log_directory (str, optional): Directory to store log files. Defaults to f"{Path(__file__).parents[1]}\\Logs".
            enable_write_log (bool, optional): Enable writing logs to file. Defaults to False.
            log_level (logging.log, optional): Logging level. Defaults to logging.INFO.

        Returns:
            logging.Logger: Initialized logger object.
        """
        log_path = f"{log_directory}\\{log_name}.log"

        logger = logging.getLogger(log_name)
        logger.setLevel(log_level)

        if enable_write_log:
            # Create Log
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)

            # Remove Old Log file
            if os.path.exists(log_path):
                os.remove(log_path)

            formatter = logging.Formatter(
                "%(asctime)s - %(name)s - %(levelname)s: %(message)s"
            )
            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)

            logger.addHandler(file_handler)

        # Adding Stream handler to print out logs additionally to the console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(ColorFormatter())
        logger.addHandler(stream_handler)

        return logger

    def get_parent_path(path: str) -> Path:
        """Get the parent folder of a given path

        If a file is given the path of the folder containing
        the file is returned, otherwise the parent folder
        is directly returned.

        Args:
            path (str): A valid file system path.

        Returns:
            A instantiated Path object of the parent folder.
        """
        return Path(path).parent


class ColorFormatter(logging.Formatter):
    """Logging Formatter to add colors and count warning/errors"""

    grey = "\x1b[90m"
    green = "\x1b[92m"
    yellow = "\x1b[93m"
    red = "\x1b[91m"
    reset = "\x1b[0m"
    format = "%(asctime)s - %(name)s - %(levelname)s: %(message)s"

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: green + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: red + format + reset,
    }

    def format(self, record):
        record.levelname = (
            "WARN" if record.levelname == "WARNING" else record.levelname
        )
        record.levelname = (
            "ERROR" if record.levelname == "CRITICAL" else record.levelname
        )
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


class Cache:
    """Cache class."""

    def __init__(self):
        self.__dict = {}

    def get(self, key: str) -> object:
        """Get a value from the cache by key.

        Args
            key (str): identifier to retrieve the value for.

        Returns
        ---------------
        The value matching the given key.
        """
        return self.__dict[key]["value"]

    def set(self, key: str, value: object) -> object:
        """Set the value in cache by key and return it.

        Args
            key (str): identifier to set the value.
            value (object): value to set.

        Returns
        -------------
        The newly set value.
        """
        self.__dict[key] = {
            "type": type(value),
            "value": value,
        }
        return value

    @property
    def all(self) -> dict[str, dict[str, object]]:
        """Return the complete cache dictionary."""
        return self.__dict

    @property
    def items(self) -> dict[str, object]:
        """Return all key-value pairs currently in the cache."""
        return {k: v["value"] for k, v in self.__dict.items()}

    def __str__(self):
        return "Cached Items: " + ", ".join(
            ["%s(%s)" % (k, str(v["value"])) for k, v in self.__dict.items()]
        )


class Hasher:
    allowed_algorithms = ["SHA256"]

    def __init__(self, algorithm: str = "SHA256"):
        if algorithm not in Hasher.allowed_algorithms:
            raise Hasher.UnknownAlgorithmExpcetion(algorithm)

        self.__algorithm = algorithm

    @property
    def algorithm(self) -> str:
        return self.__algorithm

    def hash(self, input: str) -> str:
        input_encoded = input.encode()

        if self.__algorithm.upper() == "SHA256":
            hash_object = hashlib.sha256(input_encoded)

        return hash_object

    def create_uuid(self, input: str) -> str:
        hash = self.hash(input)

        if self.__algorithm == "SHA256":
            uuid = UUID(hash.hexdigest()[::2])
        else:
            uuid = UUID(hash.hexdigest())

        return uuid

    class UnknownAlgorithmExpcetion(Exception):
        def __ini__(self, algorithm: str):
            super().__init__("Unkown algorithm: %s" % algorithm)


class JsonFileParseException(Exception):
    def __init__(self, e: Exception, file: str):
        Exception.__init__(self, str(e))
        self.file = file
        self.inner_exception = e
        self.message = str(e)

    def __str__(self):
        return self.message + textwrap.dedent(
            """
            File:       %(file)s
            Error-Type: %(type)s
            """
            % {
                "file": self.file,
                "type": type(self.inner_exception),
            }
        )


def copy_static_files(src: str, dest: str):
    static_files_src = Path(src)
    static_files_dest = Path(dest)

    shutil.copytree(static_files_src, static_files_dest, dirs_exist_ok=True)
