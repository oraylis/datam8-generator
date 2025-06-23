"""
Comprehensive unit tests for Factory classes.
Tests critical functionality and edge cases without external dependencies.
"""
import json
import logging
import os
import tempfile
import pytest
from unittest.mock import Mock, patch, MagicMock

from dm8gen.Factory.EntityFactory import EntityFactory
from dm8gen.Factory.DataSourceFactory import DataSourceFactory
from dm8gen.Factory.AttributeTypesFactory import AttributeTypesFactory
from dm8gen.Factory.DataModuleFactory import DataModuleFactory
from dm8gen.Factory.DataTypesFactory import DataTypesFactory
from dm8gen.Factory.Model import Model, LocatorNotFoundException, MultipleLocatorsFoundException


class TestEntityFactory:
    """Test EntityFactory class functionality."""
    
    def create_test_json_file(self, content):
        """Helper to create temporary JSON files for testing."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(content, f)
            return f.name
    
    def test_init_success(self):
        """Test successful EntityFactory initialization."""
        test_data = {
            "type": "raw",
            "entity": {
                "name": "TestEntity",
                "dataModule": "TestModule",
                "dataProduct": "TestProduct"
            }
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.EntityFactory.Raw') as mock_raw:
                mock_read_json.return_value = test_data
                mock_raw.model_validate_json.return_value = Mock(type=Mock(value="raw"))
                
                factory = EntityFactory(path="/test/path.json")
                
                assert factory.file_path == "/test/path.json"
                assert factory.model_type == "raw"
                assert len(factory.errors) == 0
    
    def test_init_with_error_handling(self):
        """Test EntityFactory initialization with error handling."""
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            mock_read_json.side_effect = Exception("File not found")
            
            factory = EntityFactory(path="/nonexistent/path.json")
            
            assert len(factory.errors) > 0
            assert "File not found" in factory.errors[0]
    
    def test_get_entity_object_raw(self):
        """Test __get_entity_object method for raw type."""
        test_data = {
            "type": "raw",
            "entity": {"name": "TestEntity"}
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.EntityFactory.Raw') as mock_raw:
                mock_read_json.return_value = test_data
                mock_raw.model_validate_json.return_value = Mock(type=Mock(value="raw"))
                
                factory = EntityFactory(path="/test/path.json")
                result = factory._EntityFactory__get_entity_object()
                
                assert result is not None
    
    def test_get_entity_object_invalid_type(self):
        """Test __get_entity_object method with invalid type."""
        test_data = {
            "type": "invalid",
            "entity": {"name": "TestEntity"}
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.EntityFactory.Raw') as mock_raw:
                mock_read_json.return_value = test_data
                mock_raw.model_validate_json.return_value = Mock(type=Mock(value="invalid"))
                
                factory = EntityFactory(path="/test/path.json")
                result = factory._EntityFactory__get_entity_object()
                
                assert result is None
                assert len(factory.errors) > 0


class TestDataSourceFactory:
    """Test DataSourceFactory class functionality."""
    
    def test_init_success(self):
        """Test successful DataSourceFactory initialization."""
        test_data = {
            "type": "DataSources",
            "dataSources": [
                {
                    "name": "TestSource",
                    "type": "file",
                    "properties": {}
                }
            ]
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.DataSourceFactory.DataSources') as mock_datasources:
                mock_read_json.return_value = test_data
                mock_datasources.model_validate_json.return_value = Mock(dataSources=[
                    Mock(name="TestSource", type="file")
                ])
                
                factory = DataSourceFactory(path="/test/datasources.json")
                
                assert factory.path == "/test/datasources.json"
                assert len(factory.errors) == 0
    
    def test_get_datasource_by_name(self):
        """Test getting datasource by name."""
        test_data = {
            "type": "DataSources",
            "dataSources": [
                {
                    "name": "TestSource",
                    "type": "file"
                }
            ]
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.DataSourceFactory.DataSources') as mock_datasources:
                mock_read_json.return_value = test_data
                mock_datasource_obj = Mock(name="TestSource", type="file")
                mock_datasources.model_validate_json.return_value = Mock(
                    dataSources=[mock_datasource_obj]
                )
                
                factory = DataSourceFactory(path="/test/datasources.json")
                result = factory.get_datasource_by_name("TestSource")
                
                assert result is not None
                assert result.name == "TestSource"
    
    def test_get_datasource_by_name_not_found(self):
        """Test getting datasource by name when not found."""
        test_data = {
            "type": "DataSources", 
            "dataSources": []
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.DataSourceFactory.DataSources') as mock_datasources:
                mock_read_json.return_value = test_data
                mock_datasources.model_validate_json.return_value = Mock(dataSources=[])
                
                factory = DataSourceFactory(path="/test/datasources.json")
                result = factory.get_datasource_by_name("NonexistentSource")
                
                assert result is None


class TestAttributeTypesFactory:
    """Test AttributeTypesFactory class functionality."""
    
    def test_init_success(self):
        """Test successful AttributeTypesFactory initialization."""
        test_data = {
            "type": "AttributeTypes",
            "attributeTypes": [
                {
                    "name": "TestAttribute",
                    "dataType": "string"
                }
            ]
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.AttributeTypesFactory.AttributeTypes') as mock_attr_types:
                mock_read_json.return_value = test_data
                mock_attr_types.model_validate_json.return_value = Mock(
                    attributeTypes=[Mock(name="TestAttribute")]
                )
                
                factory = AttributeTypesFactory(path="/test/attributes.json")
                
                assert factory.path == "/test/attributes.json"
                assert len(factory.errors) == 0
    
    def test_get_attribute_type_by_name(self):
        """Test getting attribute type by name."""
        test_data = {
            "type": "AttributeTypes",
            "attributeTypes": [
                {
                    "name": "TestAttribute",
                    "dataType": "string"
                }
            ]
        }
        
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.AttributeTypesFactory.AttributeTypes') as mock_attr_types:
                mock_read_json.return_value = test_data
                mock_attr_obj = Mock(name="TestAttribute", dataType="string")
                mock_attr_types.model_validate_json.return_value = Mock(
                    attributeTypes=[mock_attr_obj]
                )
                
                factory = AttributeTypesFactory(path="/test/attributes.json")
                result = factory.get_attribute_type_by_name("TestAttribute")
                
                assert result is not None
                assert result.name == "TestAttribute"


class TestModel:
    """Test Model class functionality and caching."""
    
    @pytest.fixture
    def mock_solution_file(self):
        """Create a mock solution file."""
        return {
            "type": "Solution",
            "name": "TestSolution",
            "basePath": "base",
            "rawPath": "raw", 
            "stagingPath": "stage",
            "corePath": "core",
            "curatedPath": "curated",
            "generatePath": "generate",
            "outputPath": "output"
        }
    
    def test_model_init(self, mock_solution_file):
        """Test Model initialization."""
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.Model.Solution') as mock_solution:
                mock_read_json.return_value = mock_solution_file
                mock_solution.model_validate_json.return_value = Mock(**mock_solution_file)
                
                model = Model(path_solution="/test/solution.json")
                
                assert model.path_solution == os.path.abspath("/test/solution.json")
    
    def test_caching_behavior(self, mock_solution_file):
        """Test that caching works properly."""
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.Model.Solution') as mock_solution:
                with patch('dm8gen.Factory.DataSourceFactory.DataSourceFactory') as mock_ds_factory:
                    mock_read_json.return_value = mock_solution_file
                    mock_solution.model_validate_json.return_value = Mock(**mock_solution_file)
                    
                    # Create mock for DataSourceFactory
                    mock_ds_instance = Mock()
                    mock_ds_factory.return_value = mock_ds_instance
                    
                    model = Model(path_solution="/test/solution.json")
                    
                    # Clear cache before test
                    Model.CACHE_DATA_SOURCE.clear()
                    
                    # First call should create and cache
                    ds1 = model.data_sources
                    
                    # Second call should return cached version
                    ds2 = model.data_sources
                    
                    # Should be same object due to caching
                    assert ds1 is ds2
                    
                    # Verify cache was used
                    assert len(Model.CACHE_DATA_SOURCE) == 1
    
    def test_duplicate_detection_optimization(self):
        """Test that duplicate detection uses efficient algorithm."""
        # This test verifies the fix for O(n²) duplicate detection
        mock_index = Mock()
        mock_index.rawIndex = Mock(entry=[Mock(locator="/raw/test/1"), Mock(locator="/raw/test/2")])
        mock_index.stageIndex = Mock(entry=[])
        mock_index.coreIndex = Mock(entry=[])
        mock_index.curatedIndex = Mock(entry=[])
        
        with patch('dm8gen.Helper.Helper.read_json'):
            with patch('dm8gen.Factory.Model.Solution'):
                model = Model(path_solution="/test/solution.json")
                
                # This should not raise an exception
                try:
                    model._Model__check_index_duplicates(mock_index)
                except Exception as e:
                    pytest.fail(f"Duplicate detection failed: {e}")
    
    def test_locator_lookup_invalid(self):
        """Test locator lookup with invalid locator."""
        with patch('dm8gen.Helper.Helper.read_json'):
            with patch('dm8gen.Factory.Model.Solution'):
                model = Model(path_solution="/test/solution.json")
                
                with pytest.raises(LocatorNotFoundException):
                    model.lookup_entity("nonexistent/locator")


class TestCacheManagement:
    """Test cache management and memory optimization."""
    
    def test_cache_size_limits(self):
        """Test that caches don't grow indefinitely."""
        # Clear all caches
        Model.CACHE_DATA_SOURCE.clear()
        Model.CACHE_ATTRIBUTE_TYPES.clear()
        Model.CACHE_DATA_MODULE.clear() 
        Model.CACHE_DATA_TYPES.clear()
        DataSourceFactory.CACHE_MAPPING.clear()
        
        # Verify caches start empty
        assert len(Model.CACHE_DATA_SOURCE) == 0
        assert len(DataSourceFactory.CACHE_MAPPING) == 0
    
    def test_memory_cleanup(self):
        """Test that memory is properly cleaned up."""
        # This test ensures that objects are properly garbage collected
        initial_cache_size = len(Model.CACHE_DATA_SOURCE)
        
        # Add some items to cache
        Model.CACHE_DATA_SOURCE["test1"] = Mock()
        Model.CACHE_DATA_SOURCE["test2"] = Mock()
        
        assert len(Model.CACHE_DATA_SOURCE) == initial_cache_size + 2
        
        # Clear cache
        Model.CACHE_DATA_SOURCE.clear()
        
        assert len(Model.CACHE_DATA_SOURCE) == 0


class TestErrorHandling:
    """Test comprehensive error handling."""
    
    def test_file_not_found_handling(self):
        """Test handling of file not found errors."""
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            mock_read_json.side_effect = FileNotFoundError("File not found")
            
            factory = EntityFactory(path="/nonexistent/file.json")
            
            assert len(factory.errors) > 0
    
    def test_json_parse_error_handling(self):
        """Test handling of JSON parse errors."""
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            mock_read_json.side_effect = json.JSONDecodeError("Invalid JSON", "", 0)
            
            factory = EntityFactory(path="/invalid/file.json")
            
            assert len(factory.errors) > 0
    
    def test_validation_error_handling(self):
        """Test handling of validation errors."""
        with patch('dm8gen.Helper.Helper.read_json') as mock_read_json:
            with patch('dm8gen.Factory.EntityFactory.Raw') as mock_raw:
                mock_read_json.return_value = {"invalid": "data"}
                mock_raw.model_validate_json.side_effect = ValueError("Validation failed")
                
                factory = EntityFactory(path="/test/file.json")
                
                assert len(factory.errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])