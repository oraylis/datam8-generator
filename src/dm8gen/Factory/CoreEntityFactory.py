import logging
from ..Generated.CoreModelEntry import Model as Core
from .BaseEntityFactory import BaseEntityFactory


class CoreEntityFactory(BaseEntityFactory):
    """Factory class for Core Entities."""

    def __init__(self, path: str, log_level: int = logging.INFO) -> None:
        """Initialize the CoreEntityFactory.

        Args:
            path (str): The path to the file.
            log_level (int, optional): The logging level. Defaults to logging.INFO.
        """
        super().__init__(path=path, model_class=Core, log_level=log_level)
