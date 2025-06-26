import json
from ..generated_v2.ModelDataEntity import Model
from ..Helper.Helper import Helper
import logging

logger = logging.getLogger(__name__)


class ModelDataEntityFactory:
    """Factory class for generating Stage entities."""

    file_path: str = None
    model_json: json = None
    model_object: Model = None
    model_type: str = None
    errors: list = []
    log_level: logging.log = None

    def __init__(self, path: str, log_level: logging.log = logging.INFO) -> None:
        """Initialize the StageEntityFactory.

        Args:
            path (str): Path to the JSON file containing the stage entity data.
            log_level (logging.log, optional): Logging level. Defaults to logging.INFO.
        """
        try:
            self.logger: logging.Logger = Helper.start_logger(
                self.__class__.__name__, log_level=log_level
            )
            self.file_path = path
            self.model_json = Helper.read_json(self.file_path)
            self.model_object = Model.model_validate_json(json.dumps(self.model_json))
            self.model_type = self.model_object.type.value
        except Exception as e:
            self.__error_handler(e)

    @property
    def locator(self) -> str:
        """Get the locator string for the stage entity.

        Returns:
            str: Locator string.
        """
        locator = Helper.get_locator(
            entity_type=self.model_type,
            data_module=self.model_object.entity.dataModule,
            data_product=self.model_object.entity.dataProduct,
            entity_name=self.model_object.entity.name,
        )

        return locator

    def __error_handler(self, exc: Exception):
        """Handle errors encountered during initialization.

        Args:
            exc: Exception caused the error
        """
        msg1 = f"ModelDataEntityFactory; error in {self.file_path}; {str(exc)}"
        #msg2 = str(exc)
        self.errors.append(msg1)
        #self.errors.append(msg2)
        self.logger.error(msg1)
        #self.logger.error(msg2)
