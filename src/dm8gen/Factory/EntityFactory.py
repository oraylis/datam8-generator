import logging
from typing import Union

from ..Generated.RawModelEntry import Model as Raw
from ..Generated.StageModelEntry import Model as Stage
from ..Generated.CoreModelEntry import Model as Core
from ..Generated.CuratedModelEntry import Model as Curated
from .BaseEntityFactory import BaseEntityFactory


class EntityFactory(BaseEntityFactory):
    """
    General-purpose entity factory that can handle any entity type.
    
    This factory automatically detects the entity type from the JSON data
    and creates the appropriate model object. It's useful when you don't
    know the entity type in advance.
    """

    def __init__(self, path: str, log_level: int = logging.INFO) -> None:
        """Initialize the EntityFactory.

        Args:
            path (str): The path to the file.
            log_level (int, optional): The logging level. Defaults to logging.INFO.
        """
        # We need to determine the model class dynamically
        self._determine_model_class(path)
        super().__init__(path=path, model_class=self._model_class, log_level=log_level)

    def _determine_model_class(self, path: str) -> None:
        """
        Determine the correct model class based on the entity type in the JSON.
        
        Args:
            path (str): Path to the JSON file.
        """
        from ..Helper.Helper import Helper
        
        try:
            # Read the JSON to determine the type
            model_json = Helper.read_json(path)
            entity_type = model_json.get("type", "").lower()
            
            # Map entity types to model classes
            type_to_model = {
                "raw": Raw,
                "stage": Stage,
                "core": Core,
                "curated": Curated
            }
            
            self._model_class = type_to_model.get(entity_type)
            
            if self._model_class is None:
                raise ValueError(f"Unknown entity type: {entity_type}")
                
        except Exception as e:
            # Default to Raw if we can't determine the type
            self._model_class = Raw
            raise ValueError(f"Could not determine entity type from {path}: {e}")

    def get_entity_object(self) -> Union[Raw, Stage, Core, Curated]:
        """
        Get the entity object with proper typing.

        Returns:
            Union[Raw, Stage, Core, Curated]: The validated entity object.
        """
        return self.model_object
