# Testing
The project uses [pytest] as a testing framework. All testing files are located
in the `tests` folder.

[pytest]: https://pytest.org/

## Usage
Tests are written for pytest and can simply be executed by running `pytest`.

The default for pytest target architecture is "databricks-lake" but you could swith
the target env for a particular test by calling

```sh
pytest test_050_generate.py --target=<YourTarget>
```

To write additional tests refrence the existing tests and refer otherwise to
https://docs.pytest.org/en/8.0.x/how-to/assert.html.

If tests need to be executed in a specific order, simply name the test files
accordingly since pytest simply executes the files in alphabetical order.

| Argument        | Description | Default
| :-              | :-          | :-
| `target`        | Defines which template folder should be used for testing | `databricks-lake`
| `solution-path` | Absolute path to folder containing the DataM8 solution.  | `None`

## Environment variables
The behaviour of the testing module can also be configured by defining
variables. This applies to arguments also, which can be set by prefixing
their name with `DATAM8_` and `-` replaced by `_`.

In case both are defined the cli argument will have priority.

__powershell__
```pwsh
$env:DATAM8_SOLUTION_PATH = "C:\Users\.."
```

__bash__
```sh
export DATAM8_SOLUTION_PATH="/home/..."
```

## Tests
Currently only a subset of all generator functionalities have tests defined for them.

Tests are always written & defined in two files, one file containing the test
functions, e.g. `test_10_model.py` and a second file defining the test cases, e.g.
`test_010_model_cases.py` with an analogue naming schema to the first one.

Test cases are defined using [pytest-cases].

[pytest-cases]: https://smarie.github.io/python-pytest-cases/

The `conftest.py` file defines configurations for all tests, e.g. the location
of the DataM8 solution to use for testing.

- `model` module
  - only validate functions of the `Model` class
- `helper` module
  - only the `Hasher` class
- `jinja2factory` module
  - generate functions


