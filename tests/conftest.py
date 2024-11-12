import os
import shutil
import pytest
from pickled_pipeline import Cache

TEST_CACHE_DIR = "test_pipeline_cache"


@pytest.fixture(scope="function")
def cache():
    # Set up: Create a Cache instance with a test cache directory
    cache = Cache(cache_dir=TEST_CACHE_DIR)
    yield cache
    # Tear down: Remove the test cache directory after each test
    if os.path.exists(TEST_CACHE_DIR):
        shutil.rmtree(TEST_CACHE_DIR)
