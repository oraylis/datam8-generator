import logging
from ..Generated.CuratedModelEntry import Model as Curated
from .BaseEntityFactory import BaseEntityFactory


class CuratedEntityFactory(BaseEntityFactory):
    """Factory class for Curated Entities."""

    def __init__(self, path: str, log_level: int = logging.INFO) -> None:
        """Initialize the CuratedEntityFactory.

        Args:
            path (str): The path to the file.
            log_level (int, optional): The logging level. Defaults to logging.INFO.
        """
        super().__init__(path=path, model_class=Curated, log_level=log_level)
