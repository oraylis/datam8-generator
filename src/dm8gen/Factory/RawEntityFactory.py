import logging
from ..Generated.RawModelEntry import Model as Raw
from .BaseEntityFactory import BaseEntityFactory


class RawEntityFactory(BaseEntityFactory):
    """Factory for creating Raw entities."""

    def __init__(self, path: str, log_level: int = logging.INFO) -> None:
        """Initialize RawEntityFactory.

        Args:
            path (str): The path to the JSON file.
            log_level (int, optional): The logging level. Defaults to logging.INFO.
        """
        super().__init__(path=path, model_class=Raw, log_level=log_level)
