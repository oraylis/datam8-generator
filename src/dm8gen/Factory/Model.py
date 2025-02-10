import os.path
import json
import re
import sys
from typing import Union
from dataclasses import dataclass
import textwrap
from .RawEntityFactory import RawEntityFactory
from .StageEntityFactory import StageEntityFactory
from .CoreEntityFactory import CoreEntityFactory
from .CuratedEntityFactory import CuratedEntityFactory
from .DataSourceFactory import DataSourceFactory
from .AttributeTypesFactory import AttributeTypesFactory
from .DataModuleFactory import DataModuleFactory
from .DataTypesFactory import DataTypesFactory
from .EntityFactory import EntityFactory
from ..Generated.Solution import Model as Solution
from ..Generated.Index import Model as Index
from ..Helper.Helper import Helper, JsonFileParseException
import logging

logger = logging.getLogger(__name__)


class Model:
    path_solution: str = None
    dict_solution: str = None
    errors: list = []
    log_level: logging.log = None

    def __init__(self, path_solution: str, log_level: logging.log = logging.INFO):
        self.log_level = log_level
        self.logger: logging.Logger = Helper.start_logger(
            self.__class__.__name__, log_level=log_level
        )
        self.path_solution = os.path.abspath(path_solution)
        self.dict_solution = os.path.dirname(self.path_solution)
        self.path_index = os.path.join(self.dict_solution, "index.json")

    @property
    def solution(self) -> Solution:
        try:
            solution_object = Solution.model_validate_json(
                json.dumps(Helper.read_json(path=self.path_solution))
            )
            # self.logger.info('Successfully init Solution object')
            return solution_object
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_base(self) -> str:
        try:
            base_path = self.__get_dict_path(dict_item=self.solution.basePath)
            # self.logger.info(f'Requested base path: {base_path}')
            return base_path
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_raw(self) -> str:
        try:
            raw_path = self.__get_dict_path(dict_item=self.solution.rawPath)
            self.logger.debug(f"Requested raw path: {raw_path}")
            return raw_path
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_stage(self) -> str:
        try:
            stage_path = self.__get_dict_path(dict_item=self.solution.stagingPath)
            # self.logger.info(f'Requested raw path: {stage_path}')
            return stage_path
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_core(self) -> str:
        try:
            core_path = self.__get_dict_path(dict_item=self.solution.corePath)
            # self.logger.info(f'Requested raw path: {core_path}')
            return core_path
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_curated(self) -> str:
        try:
            return self.__get_dict_path(dict_item=self.solution.curatedPath)
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_generate(self) -> str:
        try:
            generate_path = self.__get_dict_path(dict_item=self.solution.generatePath)
            # self.logger.info(f'Requested generate path: {generate_path}')
            return generate_path
        except Exception as e:
            self.__error_handler(e)

    @property
    def path_output(self) -> str:
        try:
            output_path = self.__get_dict_path(dict_item=self.solution.outputPath)
            # self.logger.info(f'Requested output path: {output_path}')
            return output_path
        except Exception as e:
            self.__error_handler(e)

    @property
    def data_sources(self) -> DataSourceFactory:
        try:
            path_datasource: str = os.path.join(
                self.__get_dict_path(self.path_base), "DataSources.json"
            )
            datasource_factory = DataSourceFactory(
                path=path_datasource, log_level=self.log_level
            )
            # self.logger.info('Successfully init Datasource Factory')
            return datasource_factory
        except Exception as e:
            self.__error_handler(e)

    @property
    def attribute_types(self) -> AttributeTypesFactory:
        try:
            path_attribute_types: str = os.path.join(
                self.__get_dict_path(self.path_base), "AttributeTypes.json"
            )
            attribute_types_factory = AttributeTypesFactory(
                path=path_attribute_types, log_level=self.log_level
            )
            # self.logger.info('Successfully init Attribute Factory')
            return attribute_types_factory
        except Exception as e:
            self.__error_handler(e)

    @property
    def data_modules(self) -> DataModuleFactory:
        try:
            path_data_modules: str = os.path.join(
                self.__get_dict_path(self.path_base), "DataModules.json"
            )
            data_modules_factory = DataModuleFactory(
                path=path_data_modules, log_level=self.log_level
            )
            # self.logger.info('Successfully init Data Module Factory')
            return data_modules_factory
        except Exception as e:
            self.__error_handler(e)

    @property
    def data_types(self) -> DataTypesFactory:
        try:
            path_data_types: str = os.path.join(
                self.__get_dict_path(self.path_base), "DataTypes.json"
            )
            data_types_factory = DataTypesFactory(
                path=path_data_types, log_level=self.log_level
            )
            # self.logger.info('Successfully init Data Types Factory')
            return data_types_factory
        except Exception as e:
            self.__error_handler(e)

    def __error_handler(self, e: Exception):
        self.errors.append(e)
        self.logger.error(str(e))

    def __get_dict_path(self, dict_item) -> str:
        # ToDo: Implement relative path (e.g. ./base)
        return str(os.path.join(self.dict_solution, dict_item))

    def get_index(self) -> Index:
        idx_json: json = Helper.read_json(self.path_index)
        _idx = Index.model_validate_json(json.dumps(idx_json))
        return _idx

    def get_raw_entity(self, path: str) -> RawEntityFactory:
        try:
            return RawEntityFactory(path=path, log_level=self.log_level)
        except Exception as e:
            self.__error_handler(e)

    def get_stage_entity(self, path: str) -> StageEntityFactory:
        try:
            return StageEntityFactory(path=path, log_level=self.log_level)
        except Exception as e:
            self.__error_handler(e)

    def get_core_entity(self, path: str) -> CoreEntityFactory:
        try:
            return CoreEntityFactory(path=path, log_level=self.log_level)
        except Exception as e:
            self.__error_handler(e)

    def get_curated_entity(self, path: str) -> CuratedEntityFactory:
        try:
            return CuratedEntityFactory(path=path, log_level=self.log_level)
        except Exception as e:
            self.__error_handler(e)

    def get_entity(self, path: str) -> EntityFactory:
        try:
            return EntityFactory(path=path, log_level=self.log_level)
        except Exception as e:
            self.__error_handler(e)

    def get_raw_entity_list(self) -> list[RawEntityFactory]:
        ls_raw_entity = []
        index = self.get_index()

        for e in index.rawIndex.entry:
            self.logger.debug(
                f"Added to raw entity list locator: {e.locator} file: {e.absPath}"
            )
            ls_raw_entity.append(self.get_raw_entity(path=e.absPath))

        return ls_raw_entity

    def get_stage_entity_list(self) -> list[StageEntityFactory]:
        ls_stage_entity = []
        index = self.get_index()

        for e in index.stageIndex.entry:
            self.logger.debug(
                f"Added to stage entity list locator: {e.locator} file: {e.absPath}"
            )
            ls_stage_entity.append(self.get_stage_entity(path=e.absPath))

        return ls_stage_entity

    def get_core_entity_list(self) -> list[CoreEntityFactory]:
        ls_core_entity = []
        index = self.get_index()

        for e in index.coreIndex.entry:
            self.logger.debug(
                f"Added to core entity list locator: {e.locator} file: {e.absPath}"
            )
            ls_core_entity.append(self.get_core_entity(path=e.absPath))

        return ls_core_entity

    def get_curated_entity_list(self) -> list[CuratedEntityFactory]:
        ls_curated_entity = []
        index = self.get_index()

        for e in index.curatedIndex.entry:
            self.logger.debug(
                f"Added to curated entity list locator: {e.locator} file: {e.absPath}"
            )
            ls_curated_entity.append(self.get_curated_entity(path=e.absPath))

        return ls_curated_entity

    def get_entity_list(
        self, regex: str = r"^([A-Za-z]*)\/([A-Za-z]*)\/([A-Za-z]*)\/([A-Za-z]*)"
    ) -> list[EntityFactory]:
        ls_entity = []
        for e in self.get_locator(regex=regex):
            self.logger.debug(
                f"Added to raw entity list locator: {e.locator} file: {e.absPath}"
            )
            ls_entity.append(self.get_entity(path=e.absPath))

        return ls_entity

    def __get_index_items(self) -> list[tuple[str, str]]:
        ls_index_items: list[tuple[str, str]] = [
            ("rawIndex", self.path_raw),
            ("stageIndex", self.path_stage),
            ("coreIndex", self.path_core),
            ("curatedIndex", self.path_curated),
        ]
        return ls_index_items

    def __get_index_entry(self, path: str) -> dict:
        ls_idx_entry = []
        for subdir, dirs, files in os.walk(path):
            json_files = [f for f in files if f.endswith(".json")]

            for file in json_files:
                abspath = os.path.join(subdir, file)

                try:
                    _js = Helper.read_json(abspath)
                except JsonFileParseException as e:
                    self.__error_handler(e)
                    continue

                locator = Helper.get_locator(
                    entity_type=_js["type"],
                    data_module=_js["entity"]["dataModule"],
                    data_product=_js["entity"]["dataProduct"],
                    entity_name=_js["entity"]["name"],
                )
                entry: dict = {
                    "absPath": abspath,
                    "name": _js["entity"]["name"],
                    "locator": locator,
                }

                # Add references field to core object
                if _js["type"] in ["core", "curated"]:
                    entry["references"] = []

                ls_idx_entry.append(entry)

        __dict: dict = {"entry": ls_idx_entry}

        return __dict

    @dataclass
    class IndexTuple:
        index: Index
        locators: list[str]

    def __get_clean_index_tuple(self) -> IndexTuple:
        try:
            ls_locators = []

            # Get current Index
            __idx = self.get_index()

            # Remove invalid index items and add valid to locators list
            for item in self.__get_index_items():
                for i in getattr(__idx, f"{item[0]}").entry:
                    if not os.path.exists(i.absPath):
                        self.logger.info(
                            f"Removing deleted file from index: {os.path.basename(i.absPath)}"
                        )
                        getattr(__idx, f"{item[0]}").entry.remove(i)
                    else:
                        ls_locators.append(i.locator)

            tpl = self.IndexTuple(index=__idx, locators=ls_locators)

            return tpl

        except Exception as e:
            self.__error_handler(e)

    def __get_refresh_index(self) -> Index:
        self.logger.info("Start index refresh")

        __idx_tuple = self.__get_clean_index_tuple()
        __idx: Index = __idx_tuple.index
        __locators: list = __idx_tuple.locators

        idx_change_time = os.path.getmtime(self.path_index)
        # idx_created_time = os.path.getctime(self.path_index)  # never used

        for item in self.__get_index_items():
            self.logger.info(f"Start refreshing index for: {item[0]}")

            for subdir, dirs, files in os.walk(item[1]):
                for file in files:
                    abspath = os.path.join(subdir, file)
                    file_name = os.path.basename(abspath)
                    file_change_time = os.path.getmtime(abspath)

                    if file_change_time > idx_change_time:
                        _js = Helper.read_json(abspath)
                        locator = Helper.get_locator(
                            entity_type=_js["type"],
                            data_module=_js["entity"]["dataModule"],
                            data_product=_js["entity"]["dataProduct"],
                            entity_name=_js["entity"]["name"],
                        )

                        # Only add if locator not already in the list
                        if locator not in __locators:
                            entry: dict = {
                                "absPath": abspath,
                                "name": _js["entity"]["name"],
                                "locator": locator,
                            }
                            e = Index.model_validate_json(json.dumps(entry))
                            getattr(__idx, f"{item[0]}").entry.append(e)
                            self.logger.info(f"Adding file to index: {file_name}")
                        else:
                            self.logger.info(
                                f"Changed File locator already in index: {file_name}"
                            )

        return __idx

    def __check_index_duplicates(self, idx: Index):
        ls_locators: list[str] = []
        for item in self.__get_index_items():
            for i in getattr(idx, f"{item[0]}").entry:
                if i.locator in ls_locators:
                    raise ValueError(
                        f"Error generating index due to duplicate locator: {i.locator} in file: {i.absPath}"
                    )
                else:
                    ls_locators.append(i.locator)

    def check_zone_for_entities(self, zone: str) -> bool:
        if zone == "raw":
            path_to_check = self.path_raw
        elif zone == "stage":
            path_to_check = self.path_stage
        elif zone == "core":
            path_to_check = self.path_core
        elif zone == "curated":
            path_to_check = self.path_curated
        else:
            raise ValueError("Unknown zone name: %s" % zone)

        return len(self.__get_index_entry(path_to_check)["entry"]) > 0

    def validate_index(self, full_index_scan=True):
        try:
            if full_index_scan or not os.path.exists(self.path_index):
                self.logger.info("Start full index generating")

                raw: dict = self.__get_index_entry(self.path_raw)
                stage: dict = self.__get_index_entry(self.path_stage)
                core: dict = self.__get_index_entry(self.path_core)
                curated: dict = self.__get_index_entry(self.path_curated)

                if self.errors:
                    raise Model.ModelParseException(inner_exceptions=self.errors)

                # Create Index
                idx_dict: dict = {
                    "type": "Index",
                    "rawIndex": raw,
                    "stageIndex": stage,
                    "coreIndex": core,
                    "curatedIndex": curated,
                }

                # Validate duplicate locators
                __idx = Index.model_validate_json(json.dumps(idx_dict))
                self.__check_index_duplicates(idx=__idx)

                # Write Index
                with open(self.path_index, "w", encoding="utf-8") as f:
                    json.dump(idx_dict, f, ensure_ascii=False, indent=4)

                self.logger.info("Index has been generated")

            else:
                idx_refreshed = self.__get_refresh_index()

                # Validate duplicate locators
                self.__check_index_duplicates(idx=idx_refreshed)

                with open(self.path_index, "w", encoding="utf-8") as f:
                    json.dump(idx_refreshed.to_dict(), f, ensure_ascii=False, indent=4)

                self.logger.info("Index has been refreshed")

        except Model.ModelParseException as e:
            self.logger.error(str(e))
            sys.exit(-1)

        except Exception as e:
            self.__error_handler(e)

    def get_locator(
        self, regex: str = r"^([A-Za-z]*)\/([A-Za-z]*)\/([A-Za-z]*)\/([A-Za-z]*)"
    ) -> list[Index]:
        if not re.search(r"^\/+.+\/.+\/.+\/.+$", re.escape(regex)):
            raise InvalidLocatorException(regex)

        ls_locators: list = []
        __idx = self.get_index()

        for item in self.__get_index_items():
            for entry in getattr(__idx, item[0]).entry:
                locator = entry.locator
                # if re.match(re.escape(regex.lower()), str(locator).lower()):
                if regex.lower() == str(locator).lower():
                    self.logger.debug(f"Matching regex locator added: {locator}")
                    ls_locators.append(entry)

        if ls_locators is None or len(ls_locators) == 0:
            raise LocatorNotFoundException(regex)

        if len(ls_locators) != 1:
            raise MultipleLocatorsFoundException("%s - %s" % (regex, str(ls_locators)))

        return ls_locators

    def lookup_entity(
        self, locator: str
    ) -> Union[
        RawEntityFactory,
        StageEntityFactory,
        CoreEntityFactory,
        CuratedEntityFactory,
    ]:
        """
        Lookup an entity object by locator

        Arguments:
            locator(str): Locator to lookup in the index, e.g. "/Raw/Sales/Customer/Customer_DE"
        """
        self.logger.debug("Start looking for locator: %s" % locator)

        if not locator.startswith("/"):
            locator = "/" + locator

        layer = locator.lower().split("/")[1]

        locator_index = self.get_locator(regex=locator)
        self.logger.debug("Locator Index: %s" % str(locator_index))

        for locator_index in self.get_locator(regex=locator):
            if layer == "raw":
                return self.get_raw_entity(path=locator_index.absPath)
            elif layer == "stage":
                return self.get_stage_entity(path=locator_index.absPath)
            elif layer == "core":
                return self.get_core_entity(path=locator_index.absPath)
            elif layer == "curated":
                return self.get_curated_entity(path=locator_index.absPath)

    def lookup_stage_entity(self, locator: str) -> StageEntityFactory:
        """
        Lookup a Stage entity object by locator

        Parameters:
            locator: str = Stage locator to lookup in index (e.g "Stage/Sales/Customer/Customer_DE")
        """
        self.logger.debug("Start looking for locator: %s" % locator)

        if locator.lower().startswith("/stage"):
            locator_index = self.get_locator(regex=locator)
            self.logger.debug("Locator Index: %s" % str(locator_index))

            for locator_index in self.get_locator(regex=locator):
                return self.get_stage_entity(path=locator_index.absPath)
        else:
            self.logger.warning(
                f"Can not return stage object as the locatore {locator} is not a stage location"
            )

    def perform_initial_checks(self, *layers) -> int:
        """Performs some simple checks to validate the model before actually
        processing templates.

        The reason to do this is that some error are hard to identify during
        template rendering, e.g. a source locator of a core entity does not
        work.

        Arguments:
            *layers: The names of layers to perform checks on, e.g. `raw`.
        """
        self.logger.debug("Start performing initial model checks")

        if "raw" in layers:
            self.__perform_raw_checks()

        if "stage" in layers:
            self.__perform_stage_checks()

        if "core" in layers:
            self.__perform_core_checks()

        if "curated" in layers:
            self.__perform_curated_checks()

        self.logger.info("Finished initial model checks")

        return 0

    def __perform_raw_checks(self) -> None:
        raw_entities: list = self.get_raw_entity_list()

        self.logger.info("Raw Entities to process: %s" % str(len(raw_entities)))

    def __perform_stage_checks(self) -> None:
        stage_entities: list = self.get_stage_entity_list()
        self.logger.info("Stage Entities to process: %s" % str(len(stage_entities)))

    def __perform_core_checks(self) -> None:
        core_entities: list = self.get_core_entity_list()
        self.logger.info("Core Entities to process: %s" % str(len(core_entities)))

        for entity in core_entities:
            table = entity.model_object.entity
            function = entity.model_object.function
            sources: list = filter(lambda x: x.dm8l != "#", function.source)

            self.logger.debug("Core Table: %s" % table.name)

            # validate source locators
            for source in sources:
                self.logger.debug("Source Entity Locator: %s" % source.dm8l)
                stage_entity = self.lookup_stage_entity(source.dm8l)
                self.logger.debug(
                    "Source Entity: %s" % stage_entity.model_object.entity.name
                )

    def __perform_curated_checks(self) -> None:
        curated_entities: list = self.get_curated_entity_list()
        self.logger.info("Curated Entities to process: %s" % str(len(curated_entities)))

        for entity in curated_entities:
            table = entity.model_object.entity
            functions = entity.model_object.function

            self.logger.debug("Curated Table: %s" % table.name)

            for function in functions:
                # validate source locators
                for source in function.source:
                    self.logger.debug("Source Entity Locator: %s" % source.dm8l)
                    core_entity = self.lookup_entity(source.dm8l)
                    self.logger.debug(
                        "Source Entity: %s" % core_entity.model_object.entity.name
                    )

    class ModelParseException(Exception):
        def __init__(
            self,
            msg="Error(s) occured during model files parsing.",
            inner_exceptions: list[Exception] = [],
        ):
            Exception.__init__(self, msg)

            self.inner_exceptions = inner_exceptions
            self.message = msg

        def get_errors(self):
            errors = [
                textwrap.dedent(
                    """
                    File:       %(file)s
                    --------------------------
                    Error:      %(message)s
                    Error-Type: %(type)s
                    """
                    % {
                        "file": e.file,
                        "type": type(e),
                        "message": str(e),
                    }
                )
                for e in self.inner_exceptions
            ]

            return self.message + "\n" + "\n".join(errors)


class LocatorNotFoundException(Exception):
    def __init__(self, locator):
        super().__init__("Locator was not found: %s" % locator)


class MultipleLocatorsFoundException(Exception):
    def __init__(self, locator):
        super().__init__("Multiple locators found for: %s" % locator)


class InvalidLocatorException(Exception):
    def __init__(self, locator):
        super().__init__("Not a valid locator: %s" % locator)
