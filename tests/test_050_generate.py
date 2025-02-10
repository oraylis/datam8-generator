from pytest_cases import fixture
from dm8gen.Factory.Jinja2Factory import Jinja2Factory


def test_validate_model(model):
    model.validate_index(full_index_scan=True)
    check_result = model.perform_initial_checks("raw", "stage", "core", "curated")

    assert check_result == 0


@fixture
def jinja_factory(model, config):
    jinja_factory = Jinja2Factory(model, config["log_level"])

    assert isinstance(jinja_factory, Jinja2Factory)
    assert jinja_factory.log_level == config["log_level"], (
        "factory log level %s does not match config %s"
        % (jinja_factory.log_level, config["log_level"])
    )

    return jinja_factory


def test_generate_template__stage(jinja_factory, config):
    jinja_factory.generate_template(
        path_template_source=config["source_path_stage"],
        path_template_destination=config["target_path_stage"],
        path_modules=config["modules_path"],
        path_collections=config["collections_path"],
    )


# def test_generate_curated_databricks_lake(jinja_factory, config):
#     jinja_factory.generate_template(
#         path_template_source=config["source_path_curated"],
#         path_template_destination=config["target_path_curated"],
#         path_modules=config["modules_path"],
#         path_collections=config["collections_path"],
#     )


def test_generate_template__output_databricks_lake(jinja_factory, config):
    jinja_factory.generate_template(
        path_template_source=config["source_path_output"],
        path_template_destination=config["target_path_output"],
        path_modules=config["modules_path"],
        path_collections=config["collections_path"],
        path_solution=config["solution_file_path"],
    )
