# Data access service

## Installation

Please install Anacronda

```
1. Run command: "conda env create -f environment.yml"
2. Install poetry (https://github.com/python-poetry/install.python-poetry.org)
3. Activate your env, conda activate data-access-service
4. poetry install / poetry update
```

## Dependency management

Poetry is use for dependency management, you can edit the file pyproject.toml for new dependency

There is one library that reference another github repo of aodn. It is written by PO to access
cloud optimized data. Please do not access s3 directly and should via this library.

Right now the version is always main branch which is not idea, we should be able to use tag version
```commandline
aodn_cloud_optimised = { git = "https://github.com/aodn/aodn_cloud_optimised.git", branch = "main" }
```

## Run the app

1. You can use startServer.sh to run the app, this will create a docker image and run the image for you
2. You can also run via python run.py, it is better you start it with a good IDE like Pycham

### Profile
> You can use the following profile, set an environment variable call PROFILE
> 1. dev (default)
> 2. edge
> 3. staging
> 4. prod

### Endpoints

| Description        | Endpoints                          | Param                                                                          | Environment                                                                   |
|--------------------|----------------------------------------|--------------------------------------------------------------------------------|-------------------------------------------------------------------------------|
| Formatted metadata | /api/v1/das/metadata/{uuid}  | none                                                                           | ALL                                                                           | 
| Raw metadata       | /api/v1/das/metadata/{uuid}/raw | none                                                                           | ALL                                                                      | 
| Subsetting         | /api/v1/das/data/{uuid} | start_date=2023-12-25T14:30:00 end_date=2024-02-25T14:30:00 start_depth=-0.06 | ALL |  