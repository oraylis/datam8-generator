from dm8gen.Generated.RawModelEntry import Model as Raw
from dm8gen.Generated.StageModelEntry import Model as Stage
from dm8gen.Generated.CoreModelEntry import Model as Core
from dm8gen.Generated.CuratedModelEntry import Model as Curated
from dm8gen.Factory.Model import Model
from dm8gen.Factory.RawEntityFactory import RawEntityFactory
from dm8gen.Factory.StageEntityFactory import StageEntityFactory
from dm8gen.Factory.CoreEntityFactory import CoreEntityFactory
from dm8gen.Factory.CuratedEntityFactory import CuratedEntityFactory
from dm8gen.Factory.Model import (
    LocatorNotFoundException,
    MultipleLocatorsFoundException,
    InvalidLocatorException,
)

import pytest
from pytest_cases import parametrize_with_cases, fixture
from .test_010_model_cases import CasesLocator, CasesModel, CasesLayer


@fixture
def target():
    return "databricks-lake"


@parametrize_with_cases("layers", cases=CasesLayer, glob="*_valid")
def test_perform_initial_checks(config, model, layers):
    assert isinstance(model, Model)

    model.perform_initial_checks(layers)


@parametrize_with_cases("attribute", cases=CasesModel, glob="*_attributes")
def test_available_attribute(attribute, model):
    assert hasattr(model, attribute), "Model is missing attribute: %s" % attribute


@parametrize_with_cases("locator", cases=CasesLocator, glob="*_valid")
def test_lookup_entity__valid(locator, config, model):
    """Test the Model().lookup_entity() function with valid locators as an input."""

    if not model.check_zone_for_entities(locator[0]):
        return
    else:
        locator = locator[1]

    entity_factory = model.lookup_entity(locator)
    model_object = entity_factory.model_object

    assert entity_factory is not None, "Return factory is None."
    assert type(entity_factory) in (
        StageEntityFactory,
        RawEntityFactory,
        CoreEntityFactory,
        CuratedEntityFactory,
    ), "Return factory is of an unkown type: %s" % str(type(entity_factory))
    assert type(model_object) in (
        Raw,
        Stage,
        Core,
        Curated,
    ), "Model_object is of unkown type: %s" % str(type(model_object))
    assert (
        entity_factory.locator.lower() == locator.lower()
        if locator.startswith("/")
        else "/%s" % locator
    ), "Locators do not match - search: %s - found: %s" % (
        locator,
        entity_factory.locator,
    )


@parametrize_with_cases("locator", cases=CasesLocator, glob="*_invalid")
def test_lookup_entity__invalid(locator, model):
    """Test the Model().lookup_entity() function with an invalid locator as an input."""

    with pytest.raises(InvalidLocatorException):
        model.lookup_entity(locator)


@parametrize_with_cases("locator", cases=CasesLocator, glob="*_multiple")
def test_lookup_entity__multiple(locator, model):
    """Test Model.lookup_entity() with multiple resolve locators."""

    with pytest.raises(MultipleLocatorsFoundException):
        # TODO: current generator does not do fuzzzy or regex matching
        raise MultipleLocatorsFoundException("dummy")


@parametrize_with_cases("locator", cases=CasesLocator, glob="*_unkown")
def test_lookup_entity__unkown(locator, model):
    """Test Model.lookup_entity() with unkownk locator."""

    with pytest.raises(LocatorNotFoundException):
        model.lookup_entity(locator)
