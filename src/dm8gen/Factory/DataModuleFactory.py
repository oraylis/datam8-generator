import json
from ..Generated.DataModules import Model as DataModules
from ..Helper.Helper import Helper
import logging

logger = logging.getLogger(__name__)


class DataModuleFactory:
    json: json = None
    data_module_object: object = None
    errors: list = []
    log_level: logging.log = None

    def __init__(self, path: str, log_level: logging.log = logging.INFO) -> None:
        try:
            self.logger: logging.Logger = Helper.start_logger(
                self.__class__.__name__, log_level=log_level
            )
            self.json = Helper.read_json(path)
            self.data_module_object = self.__get_object()

        except Exception as e:
            self.__error_handler(str(e))

    def __error_handler(self, msg: str):
        self.errors.append(f"Error DataModuleFactory: {msg}")
        self.logger.error(msg)

    def __get_object(self) -> object:

        __object = DataModules.model_validate_json(json.dumps(self.json))

        return __object

    def get_data_module(self, data_module_name) -> DataModules:

        try:
            __data_module_item = None
            __data_module_item = [
                i for i in self.data_module_object.items if i.name == data_module_name
            ][0]

            if __data_module_item is None:
                self.__error_handler(f"Found no item for module: {data_module_name}")

            return __data_module_item

        except Exception as e:
            self.__error_handler(str(e))
