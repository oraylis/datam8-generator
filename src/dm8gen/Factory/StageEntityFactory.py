import logging
from ..Generated.StageModelEntry import Model as Stage
from .BaseEntityFactory import BaseEntityFactory


class StageEntityFactory(BaseEntityFactory):
    """Factory class for generating Stage entities."""

    def __init__(self, path: str, log_level: int = logging.INFO) -> None:
        """Initialize the StageEntityFactory.

        Args:
            path (str): Path to the JSON file containing the stage entity data.
            log_level (int, optional): Logging level. Defaults to logging.INFO.
        """
        super().__init__(path=path, model_class=Stage, log_level=log_level)
