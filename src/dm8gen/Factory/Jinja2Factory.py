import glob
import jinja2
import sqlparse
import sys
import os
import json
import textwrap
import importlib.util

from .Model import Model
from autopep8 import fix_code
import importlib
from ..Helper.Helper import Helper, Path, Cache, Hasher
import logging

logger = logging.getLogger(__name__)


class Jinja2Factory:
    """Factory class for Jinja2 templates with performance optimizations."""

    # Class-level template environment cache to reuse across instances
    _template_environments: dict = {}
    _compiled_templates: dict = {}
    
    errors: list = []
    logger: logging.Logger = None
    model: Model = None
    log_level: logging.log = None

    def __init__(self, model: Model, log_level: logging.log = logging.INFO):
        """Initialize the Jinja2Factory.

        Args:
            model (Model): The model object.
            log_level (logging.log, optional): The logging level. Defaults to logging.INFO.
        """
        self.log_level = log_level
        self.logger: logging.Logger = Helper.start_logger(
            self.__class__.__name__, log_level=self.log_level
        )
        self.model = model
        
        # Instance-specific template context cache
        self._context_cache = None
        self._last_context_hash = None
        
        self.logger.debug("Successfully init Jinja2Factory with caching optimizations")

    def _get_cached_template_environment(self, search_paths: list) -> jinja2.Environment:
        """
        Get or create a cached Jinja2 environment for the given search paths.
        
        Args:
            search_paths (list): List of template search paths.
            
        Returns:
            jinja2.Environment: Cached or new Jinja2 environment.
        """
        # Create a cache key from the search paths (convert all to strings first)
        cache_key = "|".join(sorted(str(path) for path in search_paths))
        
        if cache_key not in self._template_environments:
            self.logger.debug(f"Creating new template environment for paths: {search_paths}")
            
            # Create new environment with optimizations (ensure all paths are strings)
            template_loader = jinja2.FileSystemLoader([str(path) for path in search_paths])
            template_env = jinja2.Environment(
                loader=template_loader,
                # Enable template caching
                cache_size=1000,  # Cache up to 1000 compiled templates
                auto_reload=False,  # Disable auto-reload for performance
                # Enable optimizations
                optimized=True,
                finalize=lambda x: x if x is not None else ""
            )
            
            self._template_environments[cache_key] = template_env
            self.logger.info(f"Created and cached template environment for {len(search_paths)} paths")
        else:
            self.logger.debug(f"Using cached template environment for paths: {search_paths}")
            
        return self._template_environments[cache_key]

    def _get_optimized_template_context(self, path_modules: str = None, path_solution: str = None) -> dict:
        """
        Get optimized template context with caching.
        
        Args:
            path_modules (str, optional): Path to modules.
            path_solution (str, optional): Path to solution.
            
        Returns:
            dict: Template context dictionary.
        """
        # Create a hash of the input parameters to detect changes
        context_inputs = f"{path_modules}|{path_solution}|{id(self.model)}"
        context_hash = hash(context_inputs)
        
        # Return cached context if inputs haven't changed
        if self._context_cache is not None and self._last_context_hash == context_hash:
            self.logger.debug("Using cached template context")
            return self._context_cache
        
        self.logger.debug("Building new template context")
        
        # Build base context
        cache = Cache()
        dict_modules = {"cache": cache, "Hasher": Hasher}
        dict_model = {"model": self.model}
        
        # Add solution data for template access
        dict_solution = {
            "solution": self.model.solution,
            "raw_entities": self.model.get_raw_entity_list(),
            "stage_entities": self.model.get_stage_entity_list(),
            "core_entities": self.model.get_core_entity_list(),
            "curated_entities": self.model.get_curated_entity_list(),
            "data_sources": self.model.data_sources,
            "attribute_types": self.model.attribute_types,
            "data_modules": self.model.data_modules,
            "data_types": self.model.data_types
        }
        
        # Load additional modules if specified
        if path_modules is not None:
            dict_modules.update(self._load_template_modules(path_modules))
        
        # Combine all context dictionaries
        context = dict_model | dict_modules | dict_solution
        
        # Cache the context and hash
        self._context_cache = context
        self._last_context_hash = context_hash
        
        self.logger.debug(f"Built template context with {len(context)} items")
        return context

    def _load_template_modules(self, path_modules: str) -> dict:
        """
        Load template modules with error handling and caching.
        
        Args:
            path_modules (str): Path to modules directory or file.
            
        Returns:
            dict: Dictionary of loaded modules.
        """
        modules_dict = {}
        
        try:
            path_modules = os.path.abspath(path_modules)
            
            if os.path.isfile(path_modules):
                # Single module file
                modules_dict.update(self._load_single_module(path_modules))
            elif os.path.isdir(path_modules):
                # Directory of modules
                for module_file in os.scandir(path_modules):
                    if module_file.is_file() and module_file.path.endswith(".py"):
                        modules_dict.update(self._load_single_module(module_file.path))
            else:
                self.logger.warning(f"Module path does not exist: {path_modules}")
                
        except Exception as e:
            self.logger.error(f"Error loading template modules: {e}")
            
        return modules_dict

    def _load_single_module(self, module_path: str) -> dict:
        """
        Load a single Python module for template use.
        
        Args:
            module_path (str): Path to the Python module file.
            
        Returns:
            dict: Dictionary from the module's get_dict_modules function.
        """
        try:
            # Convert Path to string to avoid PosixPath vs str comparison issues
            module_name = str(Path(module_path).stem)
            
            spec = importlib.util.spec_from_file_location(module_name, module_path)
            module = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = module
            spec.loader.exec_module(module)
            
            if hasattr(module, 'get_dict_modules'):
                return module.get_dict_modules()
            else:
                self.logger.warning(f"Module {module_name} does not have get_dict_modules function")
                return {}
                
        except Exception as e:
            self.logger.error(f"Error loading module {module_path}: {e}")
            return {}

    def _optimize_file_operations(self, path_template_destination: str):
        """
        Optimize file operations by using efficient directory creation and cleanup.
        
        Args:
            path_template_destination (str): Destination directory path.
        """
        try:
            # Create destination directory if it doesn't exist
            os.makedirs(path_template_destination, exist_ok=True)
            
            # Use more efficient file deletion - improved version of __delete_template
            if os.path.exists(path_template_destination):
                # Use faster directory walking
                for root, dirs, files in os.walk(path_template_destination):
                    # Remove all files
                    for file in files:
                        file_path = os.path.join(root, file)
                        try:
                            os.remove(file_path)
                            self.logger.debug(f"Removed file: {file}")
                        except OSError as e:
                            self.logger.warning(f"Could not remove file {file_path}: {e}")
                    
                    # Remove empty directories (bottom-up)
                    for dir_name in dirs:
                        dir_path = os.path.join(root, dir_name)
                        try:
                            if not os.listdir(dir_path):  # Only remove if empty
                                os.rmdir(dir_path)
                                self.logger.debug(f"Removed empty directory: {dir_name}")
                        except OSError:
                            # Directory not empty or other error, ignore
                            pass
                        
        except Exception as e:
            self.logger.error(f"Error in file operations optimization: {e}")

    @classmethod
    def clear_template_cache(cls):
        """Clear the template cache to free memory."""
        cls._template_environments.clear()
        cls._compiled_templates.clear()

    def get_errors(self) -> list:
        """Get errors.

        Returns:
            list: List of errors.
        """
        __ls_errors: list = self.errors + self.model.errors

        return __ls_errors

    def __error_handler(self, msg: str):
        """Handle errors.

        Args:
            msg (str): The error message.
        """
        self.errors.append(f"Error in JinjaFactory: {msg}")
        self.logger.error(msg)
        sys.exit(-1)

    def __delete_template(self, path_template: str):
        """Delete template.

        Args:
            path_template (str): The path to the template.
        """
        try:
            ls_target_files = [
                f
                for f in glob.iglob(path_template + "**/**", recursive=True)
                if os.path.isfile(f)
            ]
            for file in ls_target_files:
                self.logger.debug(f"Removing Template: {os.path.basename(file)}")
                os.remove(file)

        except Exception as e:
            self.__error_handler(str(e))

    def __get_file_name_from_line(self, identifier: str, line: str) -> str:
        """Get file name from line.

        Args:
            identifier (str): The identifier.
            line (str): The line.

        Returns:
            str: The file name.
        """
        file_name_start = line.rfind(identifier) + 1

        if "|" in line:
            file_name_end = line.rfind("|")
            file_name = line[file_name_start:file_name_end].strip()
        else:
            file_name = line[file_name_start : len(line)].strip()

        return file_name

    def __get_file_type_from_line(self, line: str) -> str:
        """Get file type from line.

        Args:
            line (str): The line.

        Returns:
            str: The file type.
        """
        file_type = ""
        if "|" in line:
            file_name_start = line.rfind("|") + 1
            file_type = line[file_name_start : len(line)].strip()

        return file_type

    def __format_file(self, file_type: str, file_path: str, file_name: str):
        """Format file.

        Args:
            file_type (str): The file type.
            file_path (str): The file path.
            file_name (str): The file name.
        """
        __file_type = file_type.lower()

        # Format json using json dumps
        if __file_type == "json":
            try:
                data = Helper.read_json(file_path)

                with open(file_path, encoding="utf-8", mode="w") as f:
                    formated_json = json.dumps(data, indent=4, ensure_ascii=False)
                    f.write(formated_json)

            except Exception as e:
                self.logger.warning(
                    f"Unable to format file {file_name} due to error {e} "
                )

        # Format python using
        elif __file_type == "py" or __file_type == "python":
            with open(file_path, "r") as f:
                data = f.read()

            try:
                autopep8_formatted_code = fix_code(data)
                with open(file_path, "w") as f:
                    f.write(autopep8_formatted_code)

            except Exception as e:
                self.logger.warning(
                    f"Unable to format file {file_name} due to error {e} "
                )

        elif __file_type == "sql":
            with open(file_path, "r") as f:
                data = f.read()

            try:
                sql_formatted_code = sqlparse.format(
                    data, reindent=True, keyword_case="upper"
                )
                with open(file_path, "w") as f:
                    f.write(sql_formatted_code)
            except Exception as e:
                self.logger.warning(
                    f"Unable to format file {file_name} due to error {e} "
                )

        else:
            self.logger.debug(
                f"Beautifier not executed due to unknow file_type: {file_type} for file: {file_name}"
            )

    def write_output(
        self, input_file_name: str, output: str, path_template_destination: str
    ):
        """Write output.

        Args:
            input_file_name (str): The input file name.
            output (str): The output.
            path_template_destination (str): The destination path for the template.
        """
        current_file = None

        try:
            for line in output.splitlines():
                strip_line = line.strip()
                if strip_line.startswith(">>>>>>>>>>"):
                    file_name = self.__get_file_name_from_line(
                        identifier=">", line=strip_line
                    )
                    file_path = os.path.join(path_template_destination, file_name)
                    directory_name = os.path.dirname(file_path)

                    if not os.path.exists(directory_name):
                        os.makedirs(directory_name)
                        self.logger.info(
                            f"Created template target directory: {path_template_destination}"
                        )

                    self.logger.debug(f"Opening file: {input_file_name}")
                    current_file = open(file_path, encoding="utf-8", mode="a")

                elif strip_line.startswith("<<<<<<<<<<"):
                    self.logger.debug(f"Closing file: {input_file_name}")
                    current_file.close()
                    current_file = None

                    # Beautifier Format File based on type
                    __file_type = self.__get_file_type_from_line(line=strip_line)
                    if __file_type:
                        __file_name = self.__get_file_name_from_line(
                            identifier="<", line=strip_line
                        )
                        __file_path = os.path.join(
                            path_template_destination, __file_name
                        )

                        self.logger.debug(
                            f"Detected file_type: {__file_type} for file: {__file_name} -> Beautfier exexcuting"
                        )
                        self.__format_file(
                            file_type=__file_type,
                            file_path=__file_path,
                            file_name=__file_name,
                        )

                else:
                    if current_file is not None:
                        current_file.write(line + "\n")
                    else:
                        if line.strip() != "":
                            self.logger.debug("WARNING: No Current File: " + line)

        except Exception as e:
            msg = """
                Error Generating Template!
                --------------------------
                Template:   {}
                Target:     {}
                Error:      {}
                Error-Type: {}

                An error occurred writing the output to the
                destination file
            """.format(
                input_file_name, current_file, e, type(e).__name__
            )

            self.__error_handler(msg)

    def generate_template(
        self,
        path_template_source: str,
        path_template_destination: str,
        path_modules: str = None,
        path_collections: str = None,
        path_solution: str = None,
    ):
        """Generate template.

        Args:
            path_template_source (str): The source path for the template.
            path_template_destination (str): The destination path for the template.
            path_modules (str, optional): The path to modules. Defaults to None.
            path_collections (str, optional): The path to collections. Defaults to None.
            path-solution (str, optional): The path to the dm8 solution file. Defaults to None.
        """
        try:
            _path_template_source = os.path.abspath(path_template_source)

            # Get Destination Folder and Source Template paths list
            _path_template_destination = os.path.abspath(path_template_destination)

            # Get List of jinja Template using globe for searching directories(Catch case for selecting a single jinja file)
            if os.path.isfile(_path_template_source):
                ls_glob = [_path_template_source]
            else:
                ls_glob = list(glob.glob(_path_template_source + "/**", recursive=True))

            # Get final template list and filesystem loader list (ignore templates from collection folder if collection parameter ist used)
            if path_collections is not None:
                _path_collections = os.path.abspath(path_collections)
                ls_templates_source = list(
                    filter(
                        lambda x: x.endswith("jinja2")
                        and not os.path.abspath(x).startswith(_path_collections),
                        ls_glob,
                    )
                )
                ls_filesystemloader = [_path_template_source, _path_collections]
            else:
                ls_templates_source = list(
                    filter(lambda x: x.endswith("jinja2"), ls_glob)
                )
                ls_filesystemloader = [_path_template_source]

            if path_solution is not None:
                ls_filesystemloader.append(Helper.get_parent_path(path_solution))

            # Use optimized template environment with caching
            templateEnv = self._get_cached_template_environment(ls_filesystemloader)
            self.logger.info(
                f"Start generating output from directory: {_path_template_source}"
            )

            # Use optimized template context with caching
            kwargs = self._get_optimized_template_context(
                path_modules=path_modules, 
                path_solution=path_solution
            )

            self.logger.debug(f"Template context contains {len(kwargs)} items")

            # Use optimized file operations
            self._optimize_file_operations(_path_template_destination)

            for template_path in ls_templates_source:
                # _path_template_source = os.path.abspath(template_path)
                _path_template = os.path.relpath(template_path, path_template_source)
                _path_template = _path_template.replace("\\", "/")

                self.logger.debug(
                    f"Start generating output for template: {_path_template}"
                )

                # # Check for valid input path
                if not os.path.exists(template_path):
                    self.__error_handler(f"Source Path does not exist {template_path}")

                if not len(self.get_errors()):
                    try:
                        template_file_path: str = ""
                        # with open(_path_template) as file_:
                        # template = Template(file_.read())
                        template = templateEnv.get_template(_path_template)
                        template_file_path = template.filename
                        template_name = os.path.basename(template_file_path)

                        self.logger.debug(f"Start generating Template: {template_name}")

                        __output = template.render(**kwargs)

                        # Write output to file
                        self.write_output(
                            input_file_name=template_name,
                            output=__output,
                            path_template_destination=_path_template_destination,
                        )

                        self.logger.info(
                            f"Finished generating output for template: {_path_template}"
                        )

                    except jinja2.TemplateSyntaxError as e:
                        msg = textwrap.dedent(
                            """
                            Error Generating Output!
                            --------------------------
                            Template:   {}
                            Error:      {}
                            Error-Type: {}
                            Error_Line: {}

                            An error occurred rendering the template with the context
                            parameters
                        """.format(
                                template_file_path, e, type(e).__name__, e.lineno
                            )
                        )

                        self.__error_handler(msg)

                    except Exception as e:
                        tb = e.__traceback__

                        traceback_summary = []
                        template_error_line = -1

                        while tb:
                            if "template" in tb.tb_frame.f_code.co_name:
                                template_error_line = tb.tb_lineno

                            traceback_summary.append(
                                "%s at %s:%s"
                                % (
                                    tb.tb_frame.f_code.co_name,
                                    tb.tb_frame.f_code.co_filename,
                                    tb.tb_lineno,
                                )
                            )
                            tb = tb.tb_next

                        msg = textwrap.dedent(
                            """

                            Error Generating Output!
                            --------------------------
                            Template:   %(template_path)s
                            Error:      %(error_message)s
                            Error-Type: %(error_type)s
                            Error-Line: %(error_line)s

                            An error occurred rendering the template with the context
                            parameters

                            Traceback
                            --------------------------
                        """
                            % {
                                "template_path": template_file_path,
                                "error_message": e,
                                "error_type": type(e).__name__,
                                "error_line": template_error_line,
                            }
                        )

                        # seperatly added to keep a consistent indentation level
                        msg += "\n".join(traceback_summary)

                        self.__error_handler(msg)

                else:
                    self.logger.error("Templates not created due to prior error")

        except Exception as e:
            self.__error_handler(str(e))
