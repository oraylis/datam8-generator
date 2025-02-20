import pathlib
import datamodel_code_generator as dcg
from hatchling.builders.hooks.plugin import interface


class GenerateDatamodelHook(interface.BuildHookInterface):
    PLUGIN_NAME = "generate_datamodel"
    CRLF = b"\r\n"
    LF = b"\n"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.__schema_dir = pathlib.Path.cwd() / "datam8-model" / "schema"
        self.__output_dir = pathlib.Path.cwd() / "src" / "dm8gen" / "Generated"
        self.__template_dir = pathlib.Path.cwd() / "template"

    def initialize(self, version, build_data):
        dcg.generate(
            input_=self.__schema_dir,
            input_file_type=dcg.InputFileType.JsonSchema,
            output=self.__output_dir,
            output_model_type=dcg.DataModelType.PydanticV2BaseModel,
            custom_template_dir=self.__template_dir,
            disable_timestamp=True,
            set_default_enum_member=True,
            use_schema_description=True,
            capitalise_enum_members=True,
            collapse_root_models=True,
            allow_extra_fields=True,
        )

        self.convert_crlf_to_lf()

    def clean(self, versions):
        self.__output_dir.rmdir()

    def convert_crlf_to_lf(self):
        for file in self.__output_dir.glob("*.py"):
            with open(file, "rb") as f:
                content = f.read()

            content = content.replace(self.CRLF, self.LF)

            with open(file, "wb") as f:
                f.write(content)
