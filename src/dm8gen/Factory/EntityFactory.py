import json
from ..Generated.RawModelEntry import Model as Raw
from ..Generated.StageModelEntry import Model as Stage
from ..Helper.Helper import Helper
import logging

logger = logging.getLogger(__name__)


class EntityFactory:
    """Factory class for Entities."""

    file_path: str = None
    model_json: json = None
    model_object: Raw = None
    model_type: str = None
    errors: list = []
    log_level: logging.log = None

    def __init__(self, path: str, log_level: logging.log = logging.INFO) -> None:
        """Initialize the EntityFactory.

        Args:
            path (str): The path to the file.
            log_level (logging.log, optional): The logging level. Defaults to logging.INFO.
        """
        try:
            self.logger: logging.Logger = Helper.start_logger(
                self.__class__.__name__, log_level=log_level
            )
            self.file_path = path
            self.model_json = Helper.read_json(self.file_path)
            self.model_object = Raw.model_validate_json(json.dumps(self.model_json))
            self.model_type = self.model_object.type.value
        except Exception as e:
            self.__error_handler(str(e))


    def __error_handler(self, msg: str):
        """Handle errors.

        Args:
            msg (str): The error message.
        """
        self.errors.append(f"Error EntityFactory with error: {msg}")
        self.logger.error(msg)

    def __get_entity_object(self) -> object:
        """Get the entity object.

        Returns:
            object: The entity object.
        """
        if self.model_type is not None:
            if self.entity_typ.lower() == "raw":
                __entity_object = Raw.model_validate_json(json.dumps(self.json))
            elif self.entity_typ.lower() == "stage":
                __entity_object = Stage.model_validate_json(json.dumps(self.model_json))
            else:
                self.__error_handler(
                    f"No valid type found in file from path {self.file_path}"
                )

            return __entity_object
