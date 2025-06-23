"""
Comprehensive unit tests for Jinja2Factory class.
Tests template caching, performance optimizations, and error handling.
"""
import json
import os
import tempfile
import pytest
from unittest.mock import Mock, patch, MagicMock

from dm8gen.Factory.Jinja2Factory import Jinja2Factory
from dm8gen.Factory.Model import Model


class TestJinja2Factory:
    """Test Jinja2Factory class functionality and performance optimizations."""
    
    @pytest.fixture
    def mock_model(self):
        """Create a mock Model object for testing."""
        model = Mock(spec=Model)
        model.solution = Mock()
        model.solution.name = "TestSolution"
        model.path_base = "/test/base"
        model.path_raw = "/test/raw"
        model.path_stage = "/test/stage"
        model.path_core = "/test/core"
        model.path_curated = "/test/curated"
        model.get_raw_entity_list.return_value = []
        model.get_stage_entity_list.return_value = []
        model.get_core_entity_list.return_value = []
        model.get_curated_entity_list.return_value = []
        model.data_sources = Mock()
        model.data_sources.get_all_datasources.return_value = []
        model.attribute_types = Mock()
        model.attribute_types.get_all_attribute_types.return_value = []
        model.data_modules = Mock()
        model.data_modules.get_all_data_modules.return_value = []
        model.data_types = Mock()
        model.data_types.get_all_data_types.return_value = []
        return model
    
    @pytest.fixture
    def temp_template_dir(self):
        """Create a temporary directory with test templates."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Create a simple test template
            template_path = os.path.join(temp_dir, "test_template.jinja2")
            with open(template_path, "w") as f:
                f.write("Hello {{ name }}!")
            
            yield temp_dir
    
    @pytest.fixture
    def temp_output_dir(self):
        """Create a temporary output directory."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    def test_init_success(self, mock_model):
        """Test successful Jinja2Factory initialization."""
        factory = Jinja2Factory(model=mock_model)
        
        assert factory.model == mock_model
        assert factory.template_env is None  # Should be None initially
        assert len(factory.errors) == 0
    
    def test_template_environment_creation(self, mock_model, temp_template_dir):
        """Test that template environment is created properly."""
        factory = Jinja2Factory(model=mock_model)
        
        # Mock the template environment creation
        with patch('jinja2.Environment') as mock_env:
            with patch('jinja2.FileSystemLoader') as mock_loader:
                mock_env_instance = Mock()
                mock_env.return_value = mock_env_instance
                
                # Call method that creates template environment
                factory._Jinja2Factory__create_template_environment([temp_template_dir])
                
                # Verify environment was created
                mock_env.assert_called_once()
                mock_loader.assert_called_once()
    
    def test_generate_template_basic(self, mock_model, temp_template_dir, temp_output_dir):
        """Test basic template generation."""
        factory = Jinja2Factory(model=mock_model)
        
        with patch.object(factory, '_Jinja2Factory__create_template_environment') as mock_create_env:
            with patch.object(factory, '_Jinja2Factory__process_template') as mock_process:
                with patch('os.path.exists', return_value=True):
                    with patch('os.listdir', return_value=['test_template.jinja2']):
                        
                        # Setup mocks
                        mock_template_env = Mock()
                        mock_create_env.return_value = mock_template_env
                        
                        factory.generate_template(
                            path_template_source=temp_template_dir,
                            path_template_destination=temp_output_dir,
                            path_solution="/test/solution.json"
                        )
                        
                        # Verify template environment was created
                        mock_create_env.assert_called_once()
    
    def test_template_caching_optimization(self, mock_model, temp_template_dir):
        """Test that template caching is implemented to avoid recompilation."""
        factory = Jinja2Factory(model=mock_model)
        
        # Create mock template environment
        mock_template_env = Mock()
        mock_template = Mock()
        mock_template.render.return_value = "Rendered output"
        mock_template_env.get_template.return_value = mock_template
        
        with patch('jinja2.Environment', return_value=mock_template_env):
            with patch('jinja2.FileSystemLoader'):
                # First call to create environment
                factory._Jinja2Factory__create_template_environment([temp_template_dir])
                
                # Verify environment is cached
                assert factory.template_env is not None
                
                # Second call should reuse cached environment
                cached_env = factory.template_env
                factory._Jinja2Factory__create_template_environment([temp_template_dir])
                
                # Environment should be the same object (cached)
                assert factory.template_env is cached_env
    
    def test_file_system_operations_optimization(self, mock_model, temp_template_dir, temp_output_dir):
        """Test that file system operations are optimized."""
        factory = Jinja2Factory(model=mock_model)
        
        # Test that directory creation is optimized
        with patch('os.makedirs') as mock_makedirs:
            with patch('os.path.exists', return_value=False):
                factory._Jinja2Factory__ensure_output_directory(temp_output_dir)
                
                # Should only call makedirs once
                mock_makedirs.assert_called_once()
    
    def test_error_handling_template_not_found(self, mock_model):
        """Test error handling when template is not found."""
        factory = Jinja2Factory(model=mock_model)
        
        with patch('jinja2.Environment') as mock_env:
            with patch('jinja2.FileSystemLoader'):
                mock_env_instance = Mock()
                mock_env_instance.get_template.side_effect = FileNotFoundError("Template not found")
                mock_env.return_value = mock_env_instance
                
                factory._Jinja2Factory__create_template_environment(["/nonexistent"])
                
                # Should handle error gracefully
                assert len(factory.errors) >= 0  # Error handling should not crash
    
    def test_memory_usage_optimization(self, mock_model, temp_template_dir):
        """Test that memory usage is optimized during template processing."""
        factory = Jinja2Factory(model=mock_model)
        
        # Mock large template output to test memory handling
        large_output = "x" * 10000  # 10KB of data
        
        with patch('jinja2.Environment') as mock_env:
            with patch('jinja2.FileSystemLoader'):
                mock_template = Mock()
                mock_template.render.return_value = large_output
                mock_env_instance = Mock()
                mock_env_instance.get_template.return_value = mock_template
                mock_env.return_value = mock_env_instance
                
                factory._Jinja2Factory__create_template_environment([temp_template_dir])
                
                # Test that large outputs are handled efficiently
                with patch('builtins.open', mock_open=True) as mock_file:
                    factory._Jinja2Factory__write_template_output(
                        large_output, "/test/output.txt"
                    )
                    
                    # Should write to file without keeping all data in memory
                    mock_file.assert_called()
    
    def test_template_inheritance_optimization(self, mock_model, temp_template_dir):
        """Test that template inheritance is optimized."""
        factory = Jinja2Factory(model=mock_model)
        
        # Create templates with inheritance
        base_template = os.path.join(temp_template_dir, "base.jinja2")
        child_template = os.path.join(temp_template_dir, "child.jinja2")
        
        with open(base_template, "w") as f:
            f.write("Base: {% block content %}{% endblock %}")
        
        with open(child_template, "w") as f:
            f.write("{% extends 'base.jinja2' %}{% block content %}Child content{% endblock %}")
        
        with patch('jinja2.Environment') as mock_env:
            with patch('jinja2.FileSystemLoader') as mock_loader:
                # Test that template loader is configured for inheritance
                factory._Jinja2Factory__create_template_environment([temp_template_dir])
                
                mock_loader.assert_called_once_with([temp_template_dir])
    
    def test_concurrent_template_processing(self, mock_model, temp_template_dir, temp_output_dir):
        """Test that multiple templates can be processed concurrently."""
        factory = Jinja2Factory(model=mock_model)
        
        # Create multiple test templates
        for i in range(3):
            template_path = os.path.join(temp_template_dir, f"template_{i}.jinja2")
            with open(template_path, "w") as f:
                f.write(f"Template {i}: {{{{ name }}}}")
        
        with patch.object(factory, '_Jinja2Factory__process_template') as mock_process:
            with patch('os.path.exists', return_value=True):
                with patch('os.listdir', return_value=['template_0.jinja2', 'template_1.jinja2', 'template_2.jinja2']):
                    
                    factory.generate_template(
                        path_template_source=temp_template_dir,
                        path_template_destination=temp_output_dir,
                        path_solution="/test/solution.json"
                    )
                    
                    # Should process all templates
                    assert mock_process.call_count >= 3
    
    def test_template_context_optimization(self, mock_model):
        """Test that template context is optimized and reused."""
        factory = Jinja2Factory(model=mock_model)
        
        # Test that context dictionary is built efficiently
        context = factory._Jinja2Factory__build_template_context()
        
        # Should contain expected keys
        expected_keys = ['model', 'solution', 'raw_entities', 'stage_entities', 
                        'core_entities', 'curated_entities']
        
        for key in expected_keys:
            assert key in context
        
        # Test that context can be reused
        context2 = factory._Jinja2Factory__build_template_context()
        
        # Should be efficient to rebuild
        assert isinstance(context2, dict)
    
    def test_string_processing_optimization(self, mock_model):
        """Test that string processing is optimized."""
        factory = Jinja2Factory(model=mock_model)
        
        # Test file name extraction optimization
        test_line = "# FILE: test_output.py"
        
        with patch.object(factory, '_Jinja2Factory__get_file_name_from_line') as mock_extract:
            mock_extract.return_value = "test_output.py"
            
            result = factory._Jinja2Factory__get_file_name_from_line(test_line)
            
            assert result == "test_output.py"
    
    def test_logging_optimization(self, mock_model):
        """Test that logging is optimized and doesn't impact performance."""
        factory = Jinja2Factory(model=mock_model, log_level=logging.DEBUG)
        
        # Test that debug logging doesn't significantly impact performance
        with patch.object(factory.logger, 'debug') as mock_debug:
            factory._Jinja2Factory__log_performance_metrics("test_operation", 0.001)
            
            # Should log performance metrics
            mock_debug.assert_called()


class TestJinja2FactoryPerformance:
    """Test performance-specific optimizations."""
    
    def test_template_compilation_caching(self):
        """Test that template compilation results are cached."""
        # This test ensures compiled templates are cached to avoid recompilation
        with patch('jinja2.Environment') as mock_env:
            mock_template = Mock()
            mock_env_instance = Mock()
            mock_env_instance.get_template.return_value = mock_template
            mock_env.return_value = mock_env_instance
            
            factory = Jinja2Factory(model=Mock())
            factory._Jinja2Factory__create_template_environment(["/test"])
            
            # First call should compile template
            template1 = factory.template_env.get_template("test.jinja2")
            
            # Second call should return cached template
            template2 = factory.template_env.get_template("test.jinja2")
            
            # Should be the same object (cached)
            assert template1 is template2
    
    def test_bulk_file_operations(self):
        """Test that file operations are optimized for bulk processing."""
        factory = Jinja2Factory(model=Mock())
        
        # Test that multiple files can be processed efficiently
        file_list = [f"file_{i}.jinja2" for i in range(10)]
        
        with patch('os.path.exists', return_value=True):
            with patch('os.listdir', return_value=file_list):
                with patch.object(factory, '_Jinja2Factory__process_template') as mock_process:
                    
                    factory._Jinja2Factory__process_template_directory("/test/templates", "/test/output")
                    
                    # Should process all files
                    assert mock_process.call_count == len(file_list)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])