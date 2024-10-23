import os
import shutil
import pytest
from cached_pipeline import Cache

# Define a temporary directory for caching during tests
TEST_CACHE_DIR = "test_pipeline_cache"


@pytest.fixture(scope="function")
def cache():
    # Set up: Create a Cache instance with a test cache directory
    cache = Cache(cache_dir=TEST_CACHE_DIR)
    yield cache
    # Tear down: Remove the test cache directory after each test
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)


def test_cache_checkpoint(cache):
    # Define a sample function to test caching
    @cache.checkpoint(name="test_function")
    def test_function(x):
        return x * x

    # Call the function for the first time
    result1 = test_function(3)
    assert result1 == 9
    # Check that the cache file was created
    cache_files = os.listdir(TEST_CACHE_DIR)
    assert len(cache_files) == 1

    # Call the function again with the same argument
    result2 = test_function(3)
    assert result2 == 9
    # Ensure the cache file count hasn't increased
    cache_files = os.listdir(TEST_CACHE_DIR)
    assert len(cache_files) == 1


def test_cache_different_arguments(cache):
    @cache.checkpoint(name="test_function")
    def test_function(x):
        return x + 1

    # Call the function with different arguments
    result1 = test_function(1)
    result2 = test_function(2)
    result3 = test_function(1)  # Should load from cache

    assert result1 == 2
    assert result2 == 3
    assert result3 == 2

    # Check that two cache files were created
    cache_files = os.listdir(TEST_CACHE_DIR)
    assert len(cache_files) == 2


def test_truncate_cache(cache):
    @cache.checkpoint(name="step1")
    def step1():
        return "result1"

    @cache.checkpoint(name="step2")
    def step2():
        return "result2"

    @cache.checkpoint(name="step3")
    def step3():
        return "result3"

    # Run all steps
    _ = step1()
    _ = step2()
    _ = step3()

    # Ensure all cache files are created
    cache_files = sorted(os.listdir(TEST_CACHE_DIR))
    assert len(cache_files) == 3

    # Truncate cache starting from step2
    cache.truncate_cache("step2")

    # Check that cache files for step2 and step3 are removed
    cache_files_after_truncate = sorted(os.listdir(TEST_CACHE_DIR))
    assert len(cache_files_after_truncate) == 1
    assert "step1" in cache_files_after_truncate[0]

    # Re-run steps to ensure they recompute and cache again
    _ = step2()
    _ = step3()
    cache_files_final = sorted(os.listdir(TEST_CACHE_DIR))
    assert len(cache_files_final) == 3


def test_cache_with_complex_arguments(cache):
    @cache.checkpoint(name="complex_function")
    def complex_function(a, b):
        return a, b  # Return a tuple of the arguments

    dict_arg = {"key1": "value1", "key2": "value2"}
    list_arg = [1, 2, 3]

    result = complex_function(dict_arg, list_arg)
    assert result == (dict_arg, list_arg)

    # Call again with the same arguments to test cache retrieval
    result_cached = complex_function(dict_arg, list_arg)
    assert result_cached == result


def test_cache_pickleable_objects(cache):
    @cache.checkpoint(name="non_pickleable_function")
    def non_pickleable_function():
        return lambda x: x * x  # Functions are not pickleable

    with pytest.raises(Exception):
        non_pickleable_function()
