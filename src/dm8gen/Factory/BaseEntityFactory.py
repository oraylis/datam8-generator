"""
Base EntityFactory class to eliminate code duplication across entity factories.
This consolidates the common functionality shared by all entity factory classes.
"""
import json
import logging
from abc import ABC
from typing import Any, Type

from ..Helper.Helper import Helper


class BaseEntityFactory(ABC):
    """
    Abstract base class for all entity factories.
    
    This class consolidates the common functionality shared by all entity factory
    classes (Raw, Stage, Core, Curated) to eliminate code duplication and ensure
    consistent behavior across all entity types.
    """
    
    def __init__(self, path: str, model_class: Type, log_level: int = logging.INFO) -> None:
        """
        Initialize the EntityFactory base class.

        Args:
            path (str): The path to the JSON file containing entity data.
            model_class (Type): The pydantic model class to use for validation.
            log_level (int, optional): The logging level. Defaults to logging.INFO.
        """
        # Initialize instance variables
        self.file_path: str = path
        self.model_json: dict = None
        self.model_object: Any = None
        self.model_type: str = None
        self.errors: list = []
        self.log_level: int = log_level
        self.model_class = model_class
        
        # Initialize logger
        self.logger: logging.Logger = Helper.start_logger(
            self.__class__.__name__, log_level=log_level
        )
        
        # Load and validate the entity data
        self._load_entity_data()
    
    def _load_entity_data(self) -> None:
        """
        Load and validate entity data from the JSON file.
        
        This method handles the common pattern of reading JSON, validating it,
        and extracting the model type that is shared across all entity factories.
        """
        try:
            # Read JSON data from file
            self.model_json = Helper.read_json(self.file_path)
            
            # Validate JSON data using the provided model class
            self.model_object = self.model_class.model_validate_json(
                json.dumps(self.model_json)
            )
            
            # Extract model type
            self.model_type = self.model_object.type.value
            
            self.logger.debug(f"Successfully loaded {self.model_type} entity from {self.file_path}")
            
        except Exception as e:
            self._error_handler(str(e))
    
    @property
    def locator(self) -> str:
        """
        Get the locator string for the entity.
        
        The locator is a standardized path-like string that uniquely identifies
        an entity within the data model hierarchy.

        Returns:
            str: The locator string in format /type/product/module/name
        """
        if not self.model_object:
            self._error_handler("Cannot generate locator: model_object is None")
            return ""
        
        try:
            locator = Helper.get_locator(
                entity_type=self.model_type,
                data_product=self.model_object.entity.dataProduct,
                data_module=self.model_object.entity.dataModule,
                entity_name=self.model_object.entity.name,
            )
            return locator
        except AttributeError as e:
            self._error_handler(f"Missing required entity attributes for locator: {e}")
            return ""
    
    @property
    def entity_name(self) -> str:
        """Get the entity name."""
        if self.model_object and hasattr(self.model_object, 'entity'):
            return self.model_object.entity.name
        return ""
    
    @property
    def data_module(self) -> str:
        """Get the data module name."""
        if self.model_object and hasattr(self.model_object, 'entity'):
            return self.model_object.entity.dataModule
        return ""
    
    @property
    def data_product(self) -> str:
        """Get the data product name."""
        if self.model_object and hasattr(self.model_object, 'entity'):
            return self.model_object.entity.dataProduct
        return ""
    
    def _error_handler(self, msg: str) -> None:
        """
        Handle errors encountered during entity processing.
        
        This provides consistent error handling across all entity factories,
        ensuring errors are both logged and stored for later inspection.

        Args:
            msg (str): The error message to handle.
        """
        error_msg = f"Error in {self.__class__.__name__}: {msg}"
        self.errors.append(error_msg)
        self.logger.error(msg)
    
    def has_errors(self) -> bool:
        """
        Check if the entity factory has encountered any errors.
        
        Returns:
            bool: True if errors have occurred, False otherwise.
        """
        return len(self.errors) > 0
    
    def get_errors(self) -> list:
        """
        Get all errors encountered during entity processing.
        
        Returns:
            list: List of error messages.
        """
        return self.errors.copy()
    
    def is_valid(self) -> bool:
        """
        Check if the entity is valid and ready for use.
        
        Returns:
            bool: True if the entity is valid, False otherwise.
        """
        return (
            self.model_object is not None and 
            self.model_type is not None and 
            not self.has_errors()
        )
    
    def __str__(self) -> str:
        """String representation of the entity factory."""
        if self.is_valid():
            return f"{self.__class__.__name__}(locator='{self.locator}')"
        else:
            return f"{self.__class__.__name__}(invalid, errors={len(self.errors)})"
    
    def __repr__(self) -> str:
        """Detailed string representation of the entity factory."""
        return (
            f"{self.__class__.__name__}("
            f"file_path='{self.file_path}', "
            f"model_type='{self.model_type}', "
            f"valid={self.is_valid()}, "
            f"errors={len(self.errors)})"
        )