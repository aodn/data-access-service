# Data access service

## Installation

Please install Anacronda

```
1. Run command: "conda env create -f environment.yml"
2. Install poetry [instruction here](https://github.com/python-poetry/install.python-poetry.org)
3. Activate your env, conda activate data-access-service
4. poetry install / poetry update
```

## Dependency management

Use poetry for dependency management, you can edit the file pyproject.toml for new dependency

There is one library that reference another github repo of aodn. It is written by PO to access
cloud optimized data. Please do not access s3 directly and should via this library.

Right now the version is always main branch which is not idea, we should be able to use tag version
```commandline
aodn_cloud_optimised = { git = "https://github.com/aodn/aodn_cloud_optimised.git", branch = "main" }
```