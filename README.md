# Pickled Pipeline

[![Build Status](https://github.com/btfranklin/pickled_pipeline/actions/workflows/python-package.yml/badge.svg)](https://github.com/btfranklin/pickled_pipeline/actions/workflows/python-package.yml) [![Supports Python versions 3.10+](https://img.shields.io/pypi/pyversions/pickled_pipeline.svg)](https://pypi.python.org/pypi/pickled_pipeline)

A Python package for caching repeat runs of pipelines that have expensive operations along the way.

## Overview

`pickled_pipeline` provides a simple and elegant way to cache the outputs of functions within a pipeline, especially when those functions involve expensive computations, such as calls to Large Language Models (LLMs) or other resource-intensive operations. By caching intermediate results, you can save time and computational resources during iterative development and testing.

## Features

- **Function Caching**: Use decorators to cache function outputs based on their inputs.
- **Checkpointing**: Assign checkpoints to pipeline steps to manage caching and recomputation.
- **Cache Truncation**: Remove cached results from a specific checkpoint onwards to recompute parts of the pipeline.
- **Input Sensitivity**: Cache keys are sensitive to function arguments, ensuring that different inputs result in different cache entries.
- **Argument Exclusion**: Exclude specific arguments from the cache key to handle unpickleable objects or sensitive data.
- **Easy Integration**: Minimal changes to your existing codebase are needed to integrate caching.

## Installation

### Using PDM

`pickled_pipeline` can be installed using PDM:

```bash
pdm add pickled_pipeline
```

### Using pip

Alternatively, you can install `pickled_pipeline` using pip:

```bash
pip install pickled_pipeline
```

## Usage

### Importing the Cache Class

First, import the `Cache` class from the `pickled_pipeline` package and create an instance of it:

```python
from pickled_pipeline import Cache

cache = Cache(cache_dir="my_cache_directory")
```

- **`cache_dir`**: Optional parameter to specify the directory where cache files will be stored. Defaults to `"pipeline_cache"`.

### Decorating Functions with `@cache.checkpoint`

Use the `@cache.checkpoint()` decorator to cache the outputs of your functions:

```python
@cache.checkpoint()
def step1_user_input(user_text):
    # Your code here
    return user_text
```

By default, the checkpoint name is the name of the function being decorated. If you wish to specify a custom name, you can pass it as an argument:

```python
@cache.checkpoint(name="custom_checkpoint_name")
def my_function(...):
    # Your code here
    pass
```

### Excluding Arguments from the Cache Key

If your function accepts arguments that are unpickleable or contain sensitive information (like database connections or API clients), you can exclude them from the cache key using the `exclude_args` parameter:

```python
@cache.checkpoint(exclude_args=['unpickleable_arg'])
def my_function(unpickleable_arg, other_arg):
    # Your code here
    pass
```

- **`exclude_args`**: A list of argument names (as strings) to exclude from the cache key. This is useful when certain arguments cannot be pickled or should not influence caching.

### Building a Pipeline

Here's an example of how to build a pipeline using cached functions:

```python
def run_pipeline(user_text):
    text = step1_user_input(user_text)
    enhanced_text = step2_enhance_text(text)
    document = step3_produce_document(enhanced_text)
    documents = step4_generate_additional_documents(document)
    summary = step5_summarize_documents(documents)
    return summary
```

### Example Functions

```python
@cache.checkpoint()
def step2_enhance_text(text):
    # Simulate an expensive operation
    enhanced_text = text.upper()
    return enhanced_text

@cache.checkpoint()
def step3_produce_document(enhanced_text):
    document = f"Document based on: {enhanced_text}"
    return document

@cache.checkpoint()
def step4_generate_additional_documents(document):
    documents = [f"{document} - Version {i}" for i in range(3)]
    return documents

@cache.checkpoint()
def step5_summarize_documents(documents):
    summary = "Summary of documents: " + ", ".join(documents)
    return summary
```

### Handling Unpickleable Objects

For functions that require unpickleable objects, such as API clients or database connections, you can exclude these from the cache key:

```python
@cache.checkpoint(exclude_args=['llm_client'])
def enhance_domain(llm_client, domain):
    # Use llm_client to perform operations
    result = llm_client.process(domain)
    return result
```

By excluding `llm_client` from the cache key, you prevent serialization errors and ensure that caching is based only on the relevant arguments.

### Running the Pipeline

```python
if __name__ == "__main__":
    user_text = "Initial input from user."
    summary = run_pipeline(user_text)
    print(summary)
```

### Truncating the Cache

If you need to recompute parts of the pipeline, you can truncate the cache from a specific checkpoint:

```python
cache.truncate_cache("step3_produce_document")
```

This will remove cached results from `"step3_produce_document"` onwards, forcing the pipeline to recompute those steps the next time it's run.

## Examples

### Full Pipeline Example

```python
from pickled_pipeline import Cache

cache = Cache(cache_dir="my_cache_directory")

@cache.checkpoint()
def step1_user_input(user_text):
    return user_text

@cache.checkpoint()
def step2_enhance_text(text):
    # Simulate an expensive operation
    enhanced_text = text.upper()
    return enhanced_text

@cache.checkpoint()
def step3_produce_document(enhanced_text):
    document = f"Document based on: {enhanced_text}"
    return document

@cache.checkpoint()
def step4_generate_additional_documents(document):
    documents = [f"{document} - Version {i}" for i in range(3)]
    return documents

@cache.checkpoint()
def step5_summarize_documents(documents):
    summary = "Summary of documents: " + ", ".join(documents)
    return summary

def run_pipeline(user_text):
    text = step1_user_input(user_text)
    enhanced_text = step2_enhance_text(text)
    document = step3_produce_document(enhanced_text)
    documents = step4_generate_additional_documents(document)
    summary = step5_summarize_documents(documents)
    return summary

if __name__ == "__main__":
    user_text = "Initial input from user."
    summary = run_pipeline(user_text)
    print(summary)
```

### Handling Different Inputs

The cache system is sensitive to function arguments. Running the pipeline with different inputs will result in new computations and cache entries.

```python
# First run with initial input
summary1 = run_pipeline("First input from user.")

# Second run with different input
summary2 = run_pipeline("Second input from user.")
```

### Using Exclude Args in Practice

Suppose you have a function that interacts with an API client:

```python
@cache.checkpoint(exclude_args=['api_client'])
def fetch_data(api_client, endpoint):
    response = api_client.get(endpoint)
    return response.json()
```

By excluding `api_client` from the cache key, you avoid serialization issues with the client object and ensure that caching is based on the `endpoint` parameter.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request.

## License

This project is licensed under the MIT License.
