# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

The ORAYLIS DataM8 Generator is a Python CLI tool that generates solution outputs for DataM8. It transforms JSON schema definitions into various output formats using Jinja2 templates. The tool handles validation, index management, and template generation for data modeling solutions.

## Commands

### Development Setup
```bash
# Clone with submodules (required for JSON schema)
git clone --recurse-submodules https://github.com/oraylis/datam8-generator.git

# Initialize submodules in existing repo
git submodule update --init --recursive

# Install dependencies and run tool
uv run dm8gen --help
```

### Core Commands
```bash
# Run the generator
uv run dm8gen -a <action> -s <solution_path> [options]

# Build the package
uv build

# Run tests (requires solution path)
uv run pytest --solution-path <path>

# Run specific test with target
pytest test_050_generate.py --target=<YourTarget>

# Linting
uvx ruff check src
```

### Actions
- `validate_index`: Validate the index file based on solution path
- `generate_template`: Generate templates from source to destination
- `refresh_generate`: Refresh index and regenerate templates

## Architecture

### Core Components

**Factory Pattern**: The system uses multiple factory classes for different entity types:
- `EntityFactory`: Base factory for all entities
- `RawEntityFactory`, `StageEntityFactory`, `CoreEntityFactory`, `CuratedEntityFactory`: Entity-specific factories
- `DataSourceFactory`, `AttributeTypesFactory`, `DataModuleFactory`, `DataTypesFactory`: Domain-specific factories
- `Jinja2Factory`: Template generation using Jinja2

**Model System**: 
- `Model` class serves as the central coordinator with caching mechanisms
- Generated classes in `src/dm8gen/Generated/` are auto-generated from JSON schemas in the `datam8-model` submodule
- Solution validation using Pydantic models

**Template System**:
- Jinja2 templates in `template/` directory support multiple output formats (pydantic, msgspec, dataclass, etc.)
- Collections and modules can be registered for template generation
- Template inheritance and block system for reusable components

### Key Directories

- `src/dm8gen/Factory/`: Factory classes for entity and template generation
- `src/dm8gen/Generated/`: Auto-generated classes from JSON schemas (via build hook)
- `src/dm8gen/Helper/`: Utility classes including logging and file operations
- `template/`: Jinja2 templates for various output formats
- `datam8-model/schema/`: Git submodule containing JSON schemas
- `tests/`: Test files using pytest-cases framework

## DataM8 Model Submodule

The `datam8-model` submodule contains JSON Schema definitions that define the structure and validation rules for DataM8 solutions. These schemas are used to generate Python classes via the build process.

### Schema Files

**Core Entity Schemas:**
- `RawModelEntry.json`: Raw data layer entities with source attributes
- `StageModelEntry.json`: Staging layer entities for data transformation
- `CoreModelEntry.json`: Core business entities with business logic
- `CuratedModelEntry.json`: Final processed data models

**Configuration Schemas:**
- `Solution.json`: Main solution file structure with paths and area types
- `Index.json`: Index structure tracking all entities across layers
- `DataSources.json`: Data source definitions and configurations
- `DataProducts.json`: Data product specifications
- `DataModules.json`: Module organization and grouping
- `DataTypes.json`: Data type definitions and mappings
- `AttributeTypes.json`: Attribute type specifications
- `DiagramDiagram.json`: Diagram and visualization definitions

### Schema Structure

Each entity schema follows a consistent pattern:
- **Type discrimination**: Enum field specifying layer (raw, stage, core, curated)
- **Entity definitions**: Core entity properties (dataModule, dataProduct, name, displayName)
- **Attribute definitions**: Column specifications with type, precision, scale, nullable properties
- **Parameter system**: Configurable parameters for entities
- **Tag system**: Metadata tags for categorization
- **Function definitions**: Layer-specific processing functions

### Build Integration

The schemas are processed during build via `hatch_build_datamodel.py`:
1. Reads JSON schemas from `datam8-model/schema/`
2. Uses `datamodel-code-generator` to create Python classes
3. Outputs generated classes to `src/dm8gen/Generated/`
4. Enables type-safe validation using Pydantic models

This ensures the generator always works with validated, strongly-typed data structures that match the schema definitions.

## DataM8 Sample Solution

The `datam8-sample-solution` repository provides a comprehensive reference implementation demonstrating DataM8's capabilities for Azure Databricks data warehousing.

### Solution Overview

**Target Platform**: Azure Databricks with Azure Data Lake Gen 2 storage
**Data Pattern**: ELT (Extract, Load, Transform) flow with medallion architecture
**Sample Domain**: AdventureWorks sales data with customer, product, and order entities

### Repository Structure

**Configuration Files:**
- `ORAYLISDatabricksSample.dm8s`: Main solution configuration
- `index.json`: Generated index tracking all entities
- `datam8.user`: User-specific configuration
- `azure-pipelines.yml`: CI/CD pipeline configuration

**Data Layer Directories:**
- `Base/`: Shared configurations (DataSources, DataProducts, DataTypes, AttributeTypes)
- `Raw/`: Source data entity definitions
- `Staging/`: Intermediate processing layer entities  
- `Core/`: Business logic and core entities
- `Curated/`: Final data mart and dimensional models

**Template System:**
- `Generate/databricks-lake/`: Template source for Databricks notebooks
- `Output/`: Generated Databricks notebooks and configuration

### Data Sources Configuration

The solution demonstrates multiple data source types:
- **AdventureWorks SQL Database**: Complete data type mapping from SQL Server to Databricks
- **Azure Data Lake Storage**: Parquet file ingestion with type conversions
- **Oracle Database**: Enterprise database connectivity example

Each data source includes comprehensive type mapping ensuring data fidelity across platforms.

### Template Architecture

**Template Organization:**
```
Generate/databricks-lake/
├── __modules/           # Python utility modules
├── __collections/       # Jinja2 macros and includes
├── notebooks/          # Databricks notebook templates
└── databricks.yml.jinja2  # Databricks configuration
```

**Notebook Generation Zones:**
- `000-utils/`: Utility notebooks for setup and migration framework
- `010-raw/`: Data extraction from sources to Delta tables with partitioning
- `020-stage/`: Data validation, type casting, and poison table handling
- `030-core/`: Business rule implementation and entity relationships
- `031-curated/`: Dimensional modeling with SCD (Slowly Changing Dimensions)

### Entity Examples

**Raw Layer**: Direct source mappings with all source attributes
**Core Layer**: Business entities with relationships and computed fields
**Curated Layer**: Dimensional models with:
- SID (Surrogate ID) generation
- SCD Type 1 and Type 2 handling
- Business key management
- Custom compute functions for complex transformations

### Advanced Features

**Data Quality**: Poison table pattern for invalid data isolation
**Delta Processing**: Incremental loading based on delta tags
**Schema Evolution**: Flexible schema handling in raw layer
**Audit Trail**: Comprehensive timestamp tracking across all layers
**Custom Functions**: Python business logic integration (e.g., `Customer_Compute.py`)

### Generated Output

The solution generates production-ready Databricks notebooks with:
- DDL scripts for table creation with proper data types
- DML scripts for incremental data processing
- Structured commenting and metadata
- Error handling and logging
- Performance optimizations (partitioning, indexing)

## Template Generation Results

When executing the template generation command, DataM8 produces a complete set of Databricks assets:

### Generation Process

The generator processes entities in layers:
1. **Index Generation**: Updates `index.json` with all entity references and dependencies
2. **Template Processing**: Renders Jinja2 templates for each entity across all layers
3. **File Organization**: Creates structured output following Databricks best practices

### Generated Assets

**Databricks Bundle Configuration** (`databricks.yml`):
- Multi-environment setup (dev-personal, dev, prod)
- Service principal authentication
- Workspace configuration with proper permissions
- Variable management for catalog names, storage accounts, and environment settings

**Utility Notebooks** (`000-utils/`):
- `MigrationFramework.py`: Core framework for migration operations
- `Setup_Databricks.py`: Environment initialization and configuration

**Data Processing Notebooks**:

**Raw Layer** (`010-raw/dml/`):
- Data extraction from configured sources (SQL Server, Oracle, Data Lake)
- Delta table creation with partitioning by date and timestamp
- Incremental loading based on delta tags
- Source system connection management

**Stage Layer** (`020-stage/`):
- **DDL**: Schema definitions with proper Spark data types
- **DML**: Data validation, type casting, and poison table handling
- Column selection and transformation from raw to typed structures
- Data quality checks with error isolation

**Core Layer** (`030-core/`):
- Business entity definitions with relationships
- Data transformations and business rule implementation
- Surrogate key generation and management
- Cross-entity joins and computed fields

**Curated Layer** (`031-curated/`):
- Dimensional model implementation
- SCD (Slowly Changing Dimensions) Type 1 and Type 2 support
- Business key and surrogate key management
- Custom business function integration (e.g., `Customer_Compute.py`)

### Code Generation Features

**Template Capabilities**:
- Dynamic schema generation based on entity definitions
- Conditional logic for different entity types and attributes
- Macro system for reusable code patterns
- Integration with custom Python modules and helper functions

**Generated Code Quality**:
- Proper PySpark imports and type definitions
- Databricks-specific magic commands and utilities
- Widget-based parameterization for runtime flexibility
- Environment-aware configuration management
- Structured logging and error handling

**Business Logic Integration**:
- Custom compute functions as separate notebooks
- Complex joins across multiple entities
- Data transformation pipelines with proper error handling
- Integration with migration framework for deployment management

This demonstrates DataM8's ability to generate enterprise-ready data pipelines from declarative entity definitions.

## Development Workflow

### Essential Development Commands

```bash
# Run tests with sample solution
uv run pytest --solution-path /path/to/datam8-sample-solution

# Run specific test module
uv run pytest tests/test_010_model.py --solution-path /path/to/datam8-sample-solution

# Run with verbose output
uv run pytest -v --solution-path /path/to/datam8-sample-solution

# Lint code
uvx ruff check src

# Format code
uvx ruff format src

# Build package (triggers code generation)
uv build

# Clean build artifacts
rm -rf dist/ src/dm8gen/Generated/*.py
```

### Testing Framework

**Structure**: Tests use pytest with pytest-cases for parameterized testing across entity types
- Test files: `test_XXX_module.py` (test functions)
- Case files: `test_XXX_module_cases.py` (test data/scenarios)
- Environment variables: `DATAM8_SOLUTION_PATH`, `DATAM8_TARGET` etc.

**Key Test Patterns**:
```python
# Parameterized testing across layers
@parametrize("layer", ["raw", "stage", "core", "curated"])

# Test case classes for entity scenarios
class CasesLayer:
    @parametrize("layer", ["raw", "stage", "core", "curated"])
```

**Test Modules**:
- `test_010_model.py`: Model validation and entity processing
- `test_020_helper.py`: Helper utilities (Hasher, Cache, logging)
- `test_050_generate.py`: Template generation end-to-end

### Error Handling Patterns

**Custom Exceptions**:
- `JsonFileParseException`: JSON parsing errors with file context
- Schema validation failures: Handled by Pydantic with detailed error messages

**Debugging Commands**:
```bash
# Run with debug logging
uv run dm8gen -a validate_index -s solution.dm8s -l DEBUG

# Trace template generation issues
uv run dm8gen -a refresh_generate -s solution.dm8s -l DEBUG [other args]
```

**Common Debugging Areas**:
- `Model.validate_index()`: Entity validation and relationship checking
- `Jinja2Factory.generate_template()`: Template rendering issues
- Schema loading: `src/dm8gen/Generated/` build artifacts

### Code Architecture Patterns

**Factory Pattern Implementation**:
```python
# Each entity type has a dedicated factory
RawEntityFactory, StageEntityFactory, CoreEntityFactory, CuratedEntityFactory
DataSourceFactory, AttributeTypesFactory, DataModuleFactory, DataTypesFactory

# Base factory with common functionality
EntityFactory: Base class with validation and common operations
```

**Model Class Caching**:
```python
# Class-level caches for performance
CACHE_DATA_SOURCE: dict = {}
CACHE_ATTRIBUTE_TYPES: dict = {}
CACHE_DATA_MODULE: dict = {}
CACHE_DATA_TYPES: dict = {}
```

**Template System Organization**:
```
template/
├── pydantic/          # Pydantic model templates
├── pydantic_v2/       # Pydantic v2 specific templates
├── dataclass.jinja2   # Dataclass generation
├── Enum.jinja2        # Enum generation
└── root.jinja2        # Root model template
```

### Build System Integration

**Custom Hatch Hook** (`hatch_build_datamodel.py`):
1. Reads JSON schemas from `datam8-model/schema/`
2. Generates Python classes using `datamodel-code-generator`
3. Outputs to `src/dm8gen/Generated/`
4. Adds GPL license headers
5. Normalizes line endings (CRLF → LF)

**Generated Code Dependencies**:
- Classes in `Generated/` are auto-generated - never edit manually
- Changes require schema updates in `datam8-model` submodule
- Build hook runs automatically during `uv build`

### Helper Utilities Reference

**Core Functions** (`src/dm8gen/Helper/Helper.py`):
```python
Helper.read_json(path)           # Safe JSON reading with validation
Helper.validate_json_schema()    # Schema validation using jsonschema
Helper.start_logger()            # Configurable logging setup
Helper.get_locator()             # Generate entity locator strings
Helper.coalesce()                # First non-None value utility

Hasher.hash()                    # SHA256 hashing
Hasher.create_uuid()             # UUID from hash values

Cache.get()/set()                # Simple key-value caching
```

**Logging Configuration**:
- Color-coded console output (DEBUG=grey, INFO=green, WARN=yellow, ERROR=red)
- File output with rotation
- Configurable levels via CLI (`-l DEBUG|INFO|WARN|ERROR|CRITICAL`)

### Development Best Practices

**Code Conventions**:
- Use existing factory patterns for new entity types
- Follow Pydantic validation patterns in Generated classes
- Implement caching for expensive operations (see Model class)
- Use Helper utilities for file operations and logging

**Template Development**:
- Place reusable macros in `__collections/macro/`
- Use `__modules/` for Python helper functions
- Follow existing naming patterns (`ddl_`, `dml_` prefixes)
- Test templates with sample solution before deploying

**Schema Changes**:
1. Update schemas in `datam8-model` submodule
2. Rebuild to regenerate classes: `uv build`
3. Update factory classes if new properties added
4. Run full test suite to validate changes

This provides everything needed for immediate development productivity in any new session.

## Build Process

The project uses a custom Hatch build hook (`hatch_build_datamodel.py`) that generates Python classes from JSON schemas in the submodule using `datamodel-code-generator`.

## Solution Structure

DataM8 solutions follow a structured format with:

### Solution File (.dm8s)
Main configuration file defining paths for different data layers:
- `basePath`, `rawPath`, `stagingPath`, `corePath`, `curatedPath`: Directory structure
- `generatePath`, `diagramPath`, `outputPath`: Generation and output paths
- `AreaTypes`: Mapping of data processing stages

### Data Layer Organization
- **Raw**: Source data definitions with entity attributes and types
- **Staging**: Intermediate processing layer
- **Core**: Business logic and core entities  
- **Curated**: Final processed data models
- **Base**: Shared configurations (AttributeTypes, DataProducts, DataSources, DataTypes)

### Entity Definitions
JSON files defining data entities with:
- Entity metadata (dataModule, dataProduct, name, displayName)
- Attribute definitions (name, type, precision, scale, nullable, etc.)
- Tags and parameters for additional configuration

### Index Management
The `index.json` file tracks all entities with absolute paths and locators for efficient processing.

## Example Commands with Sample Solution

```bash
# Validate index using sample solution
uv run dm8gen -a validate_index -s /path/to/ORAYLISDatabricksSample.dm8s

# Generate templates from sample solution
uv run dm8gen --action refresh_generate --path-solution "/mnt/c/Users/f.kayser/Projects/ORAYLIS/Automation/Repos/datam8-sample-solution/ORAYLISDatabricksSample.dm8s" --path-template-source "/mnt/c/Users/f.kayser/Projects/ORAYLIS/Automation/Repos/datam8-sample-solution/Generate/databricks-lake" --path-template-destination "/mnt/c/Users/f.kayser/Projects/ORAYLIS/Automation/Repos/datam8-sample-solution/Output" --path-modules "/mnt/c/Users/f.kayser/Projects/ORAYLIS/Automation/Repos/datam8-sample-solution/Generate/databricks-lake/__modules" --path-collections "/mnt/c/Users/f.kayser/Projects/ORAYLIS/Automation/Repos/datam8-sample-solution/Generate/databricks-lake/__collections"

# Run tests with sample solution
uv run pytest --solution-path /path/to/datam8-sample-solution
```

## Development Notes

- Always ensure submodules are up to date when working with schema changes
- The `src/dm8gen/Generated/` directory is auto-generated during build - don't edit manually
- Solution files must be in JSON format and pass Pydantic validation
- Template generation supports modular architecture with collections and custom modules
- Sample solution provides reference implementation with Databricks templates and notebooks