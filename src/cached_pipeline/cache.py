import os
import pickle
import hashlib
from functools import wraps


class Cache:
    def __init__(self, cache_dir="pipeline_cache"):
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)

    def checkpoint(self, name):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                # Create a unique key based on the checkpoint name and function arguments
                key_input = (name, args, kwargs)
                key_hash = hashlib.md5(pickle.dumps(key_input)).hexdigest()
                cache_filename = f"{name}__{key_hash}.pkl"
                cache_path = os.path.join(self.cache_dir, cache_filename)
                if os.path.exists(cache_path):
                    with open(cache_path, "rb") as f:
                        result = pickle.load(f)
                    print(f"[{name}] Loaded result from cache.")
                else:
                    result = func(*args, **kwargs)
                    with open(cache_path, "wb") as f:
                        pickle.dump(result, f)
                    print(f"[{name}] Computed result and saved to cache.")
                return result

            return wrapper

        return decorator

    def truncate_cache(self, starting_from_checkpoint_name):
        # List all cache files in the cache directory
        cache_files = sorted(os.listdir(self.cache_dir))
        delete_flag = False
        for file_name in cache_files:
            if file_name.endswith(".pkl"):
                # Extract the checkpoint name from the file name
                checkpoint_name = file_name.split("__")[0]
                if checkpoint_name == starting_from_checkpoint_name:
                    delete_flag = True
                if delete_flag:
                    cache_path = os.path.join(self.cache_dir, file_name)
                    os.remove(cache_path)
                    print(f"Removed cache for checkpoint '{checkpoint_name}'")
