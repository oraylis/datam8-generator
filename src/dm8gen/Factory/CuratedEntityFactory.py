import json
from ..Generated.CuratedModelEntry import Model as Curated
from ..Helper.Helper import Helper
import logging

logger = logging.getLogger(__name__)


class CuratedEntityFactory:
    """Factory class for Core Entities."""

    file_path: str = None
    model_json: json = None
    model_object: Curated = None
    model_type: str = None
    errors: list = []
    log_level: logging.log = None

    def __init__(self, path: str, log_level: logging.log = logging.INFO) -> None:
        """Initialize the CoreEntityFactory.

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
            self.model_object = Curated.model_validate_json(json.dumps(self.model_json))
            self.model_type = self.model_object.type.value
        except Exception as e:
            self.__error_handler(str(e))

    @property
    def locator(self) -> str:
        """Get the locator.

        Returns:
            str: The locator.
        """
        locator = Helper.get_locator(
            entity_type=self.model_type,
            data_product=self.model_object.entity.dataProduct,
            data_module=self.model_object.entity.dataModule,
            entity_name=self.model_object.entity.name,
        )

        return locator

    # def get_history(self, enum: History) -> str:
    #     return enum.value

    def __error_handler(self, msg: str):
        """Handle errors.

        Args:
            msg (str): The error message.
        """
        self.errors.append(f"Error CoreEntityFactory with error: {msg}")
        self.logger.error(msg)
