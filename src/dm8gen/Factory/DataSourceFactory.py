import json
from ..Generated.DataSources import Model as DataSources
from ..Helper.Helper import Helper
from .CacheManager import OptimizedCache
import logging

logger = logging.getLogger(__name__)


class DataSourceFactory:
    """Factory class for Data Sources with optimized caching and error handling."""

    # Replace unbounded dict with optimized cache
    _mapping_cache = OptimizedCache(max_size=1000, default_ttl=3600)

    json: json = None
    source_object: object = None
    errors: list = []
    log_level: logging.log = None

    def __init__(
        self, path: str, log_level: logging.log = logging.INFO
    ) -> None:
        """Initialize the DataSourceFactory.

        Args:
            path (str): The path to the file.
            log_level (logging.log, optional): The logging level. Defaults to logging.INFO.
        """
        self.path = path
        self.log_level = log_level
        self.logger: logging.Logger = Helper.start_logger(
            self.__class__.__name__, log_level=log_level
        )
        
        try:
            self.json = Helper.read_json(path)
            self.source_object = self.__get_object()
            self.logger.debug(f"Successfully initialized DataSourceFactory from {path}")

        except FileNotFoundError:
            self.__error_handler(f"Data sources file not found: {path}")
        except json.JSONDecodeError as e:
            self.__error_handler(f"Invalid JSON in data sources file {path}: {e}")
        except Exception as e:
            self.__error_handler(f"Failed to initialize DataSourceFactory: {e}")

    def __error_handler(self, msg: str):
        """Handle errors.

        Args:
            msg (str): The error message.
        """
        self.errors.append(f"Error DataSourceFactory: {msg}")
        self.logger.error(msg)

    def __get_object(self) -> object:
        """Get the data source object.

        Returns:
            object: The data source object.
        """
        __object = DataSources.model_validate_json(json.dumps(self.json))
        return __object

    def get_datasource(self, source_name) -> DataSources:
        """Get the data source.

        Args:
            source_name: The name of the data source.

        Returns:
            DataSource: The data source.
        """
        try:
            __source_item = None
            __source_item = [
                i for i in self.source_object.items if i.name == source_name
            ][0]

            if __source_item is None:
                self.__error_handler(
                    f"Found no Source item for source name: {source_name}"
                )

            return __source_item

        except Exception as e:
            self.__error_handler(e)

    def get_datasource_list(self) -> list[DataSources]:
        """Get a list of all data sources.

        Returns:
            list[DataSource]: A list of data sources.
        """
        try:
            return self.source_object.items
        except Exception as e:
            self.__error_handler(e)

    def get_datasource_target_type(self, source_name: str, source_type) -> str:
        """Get the target type of the data source with optimized caching.

        Args:
            source_name (str): The name of the data source.
            source_type: The source type.

        Returns:
            str: The target type of the data source.
        """
        cache_key = f"{source_name}:{source_type}"
        
        # Check optimized cache
        cached_result = self._mapping_cache.get(cache_key)
        if cached_result is not None:
            self.logger.debug(f"Using cached mapping for {cache_key}")
            return cached_result

        try:
            source_item = self.get_datasource(source_name=source_name)
            if source_item is None:
                self.__error_handler(f"Data source '{source_name}' not found")
                return ""
            
            # Use more efficient lookup
            target_types = [
                mapping.targetType
                for mapping in source_item.dataTypeMapping
                if mapping.sourceType == source_type
            ]
            
            if len(target_types) == 1:
                target_type = target_types[0]
                self.logger.debug(
                    f"Mapped {source_name} from {source_type} to {target_type}"
                )
            elif len(target_types) == 0:
                target_type = ""
                self.logger.warning(
                    f"No mapping found for {source_name} sourceType: {source_type}"
                )
            else:
                target_type = target_types[0]  # Use first match
                self.logger.warning(
                    f"Multiple mappings found for {source_name} sourceType: {source_type}, using first: {target_type}"
                )

            # Cache the result
            self._mapping_cache.set(cache_key, target_type)
            return target_type

        except AttributeError as e:
            self.__error_handler(f"Invalid data source structure for {source_name}: {e}")
            return ""
        except Exception as e:
            self.__error_handler(f"Error getting target type for {source_name}:{source_type} - {e}")
            return ""
    
    @classmethod
    def clear_mapping_cache(cls):
        """Clear the mapping cache."""
        cls._mapping_cache.clear()
        logger.debug("Cleared DataSourceFactory mapping cache")
    
    @classmethod
    def get_cache_stats(cls):
        """Get cache statistics."""
        return cls._mapping_cache.get_stats()
    
    def get_all_datasources(self):
        """Get all data sources - alias for get_datasource_list for consistency."""
        return self.get_datasource_list()
