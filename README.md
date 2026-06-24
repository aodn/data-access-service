[![Language: Python](https://img.shields.io/badge/Language-Python-blue.svg)](https://www.python.org/)

# Data Access Service

## Run the app with Docker

Simply run `./startServer.sh` to run the app, this will create a docker image and run the image for you.

Host will be `http://localhost:8000`.

## Run the app for development

### Requirements

- Conda (recommended for creating a virtual environment)

1. **Install Conda** (if not already installed):

   Follow the instructions at [Conda Installation](https://docs.conda.io/projects/conda/en/latest/user-guide/install/index.html).

2. **Create Conda virtual environment:**

   ```bash
   $ conda env create -f environment.yml
   ```

### Dependency management

Poetry is used for dependency management, the `pyproject.toml` file is what is the most important, it will orchestrate the project and its dependencies.

You can update the file `pyproject.toml` for adding/removing dependencies by using

```bash
$ poetry add <pypi-dependency-name> # e.g poetry add numpy
$ poetry remove <pypi-dependency-name> # e.g. poetry remove numpy
```

You must update the `poetry.lock` file after modifying `pyproject.toml` with `poetry lock` command and commit it, otherwise CI and Docker builds will fail. To update all dependencies, use `poetry update` command.

There is a library `aodn_cloud_optimised`, which referencing direct AODN's GitHub repository as source instead of `PyPi`. It is written by PO to access
cloud optimized data. Please do not access S3 directly and should via this library.

Right now the version is always `main` branch which is not idea, we should be able to use tag version

```bash
aodn_cloud_optimised = { git = "https://github.com/aodn/aodn_cloud_optimised.git", tag = "v0.1.44" }
```

### Error in dependencies

You may try to clean the cache by using the following command

```commandline
poetry cache clear --all PyPI
poetry env remove --all
poetry lock
poetry install
```

### Installation and Usage

1. **Activate Conda virtual environment:**

   ```bash
   $ conda activate data-access-service
   ```

2. **Install dependencies using Poetry:**

   ```bash
   # after cloning the repo with git clone command
   $ cd data-access-service
   $ poetry install
   ```

3. **Run the app:**
   In project root folder, create a '.env' file, which contains your API key, e.g.:

   ```
   API_KEY="your_actual_api_key_here"
   ```

   Host will be `http://localhost:5000` and default profile is DEV, make sure you export the AWS key of edge env
   else you will see s3 upload failed for some operations

      ```bash
      export AWS_ACCESS_KEY_ID="???"
      export AWS_SECRET_ACCESS_KEY="???"
      export AWS_SESSION_TOKEN="???"
      $ python -m data_access_service.server
      ```

### Code formatting

The command below is for manual checks; checks are also executed when you run `git commit`.

The configurations for pre-commit hooks are defined in `.pre-commit-config.yaml`.

To install hooks:

```shell
pre-commit install --install-hooks -t post-checkout -t post-merge
```

To run checks manually:

```shell
pre-commit run --all-files
```

To verify `poetry.lock` is in sync with `pyproject.toml`:

```shell
pre-commit run poetry-lock --all-files
```

## Environment variables

In the root directory of the project, create a `.env` file.

### Profiles

You can use one of the following profiles, set an environment variable call `PROFILE` in `.env`

> 1. dev (default)
> 2. edge
> 3. staging
> 4. prod

E.g

```shell
PROFILE=edge
```

### Endpoints

| Description                                                      | Endpoints                                                  | Param                                                                                                                                                                                     | Environment |
|------------------------------------------------------------------|------------------------------------------------------------| ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |-------------|
| Health check                                                     | (GET) /api/v1/das/health                                   | none                                                                                                                                                                                      | ALL         |
| Formatted metadata list                                          | (GET) /api/v1/das/metadata                                 | none                                                                                                                                                                                      | ALL         |
| Formatted metadata                                               | (GET) /api/v1/das/metadata/{uuid}                          | none                                                                                                                                                                                      | ALL         |
| Raw metadata                                                     | (GET) /api/v1/das/metadata/{uuid}/raw                      | none                                                                                                                                                                                      | ALL         |
| Get Python notebook url                                          | (GET) /api/v1/das/data/{uuid}/notebook_url                 | none                                                                                                                                                                                      | ALL         |
| Subsetting                                                       | (GET) /api/v1/das/data/{uuid}/{dataset}                                | start_date=2023-12-25T14:30:00, end_date=2024-02-25T14:30:00, start_depth=-0.06, f=netcdf or json, columns=TIME&columns=DEPTH&columns=LONGITUDE&columns=LATITUDE (array of column return) | ALL         |
| Check has data or not within a timeframe                         | (GET) /api/v1/das/data/{uuid}/{dataset}/has_data                     | start_date=2023-12-25T14:30:00, end_date=2024-02-25T14:30:00                                                                                                                              | ALL         |
| Get temporal extent                                              | (GET) /api/v1/das/data/{uuid}/{dataset}/temporal_extent              | none                                                                                                                                                                                      | ALL         |
| Get values for indexing                                          | (GET) /api/v1/das/data/{uuid}/{key}/indexing_values        | key=radar_CoffsHarbour_wind_delayed_qc.zarr, start_date=2023-12-25T14:30:00, end_date=2024-02-25T14:30:00                                                                                 |             |
| Get sites and their locations that contain data in a time window | (GET) /api/v1/das/data/feature-collection/{product}        | start_date=2023-12-25T14:30:00, end_date=2024-02-25T14:30:00 (both optional)                                                                                                              | ALL         |
| Get a single site's observation timeseries in a time window      | (GET) /api/v1/das/data/feature-collection/{product}/{site} | start_date=2023-12-25T14:30:00, end_date=2024-02-25T14:30:00 (both optional)                                                                                                              | ALL         |
| Get latest available observation time for a product              | (GET) /api/v1/das/data/feature-collection/{product}/latest | none                                                                                                                                                                                      | ALL         |
| Generate a pmtiles for the given product                         | (PUT) /api/v1/das/pmtiles/{uuid}/{dataset}                 | none                                                                                                                                                                                      | ALL         |

`{product}` must be one of: `mooring`, `wave-buoy`.

### Running Tests

To run the tests for the project:

```shell
poetry run python -m unittest discover
```

This will discover and run all the test cases in your project.

If you have "ModuleNotFoundError" or similar issues, you may need to install dependencies before running the tests:

```shell
poetry install
```

# Batch jobs

Another part of this project is to run batch jobs for dataset subsetting.

### Local running testing

- If you want to test the batch job codes locally, (running in your local machine)
  Please export aws environment variables first (or use profile etc..) (for example, if use edge, please go to aws access portal, and pick AODN-Edge -> AodnAdminAccess)
  and also export `AWS_BATCH_JOB_ID` (please go to batch console to copy an existing job id).
  After several exporting, make sure your terminal is at the root folder of this project. Then please run:

```shell
./entry_point.py
```

### aws running testing

- If you want to test the batch job codes in AWS, (running in AWS Batch), please :
  1. Build the docker image and push it to ECR (havier-test-ecr). Please do it by following the instructions in the havier-test-ecr repo by clicking button "View push commands" at the top right corner.
  2. Open the ogc-api project locally
  3. In class DatasetDownloadEnums of the ogc-api project, go to JobDefinition enum and change the value of GENERATE_CSV_DATA_FILE from "generate-csv-data-file" into "generate-csv-data-file-dev" (add "-dev" at the end of the string).
  4. run the ogc-api project locally and run the portal project locally (make sure your local portal is pointing to the local ogc-api project).
  5. Go to localhost:5173, and navigate to an IMOS hosted dataset. On the detail page, select any date range and / or spatial area, and click "Download" button. This will trigger the batch job.
  6. For dev stage, if you cannot receive the email, please make sure the email address you provided has added into the AWS SES verified email list.

## Styles

We are using [material ui](https://mui.com/material-ui/) and our configuration theme file it's in `AppTheme.ts`

## Automated UI tests

See [`playwright/README.md`](./playwright/README.md).

## Commit

We are using [gitmoji](https://gitmoji.dev/)(OPTIONAL) with husky and commitlint. Here you have an example of the most used ones:

- :art: - Improving structure/format of the code.
- :zap: - Improving performance.
- :fire: - Removing code or files.
- :bug: - Fixing a bug.
- :ambulance: - Critical hotfix.
- :sparkles: - Introducing new features.
- :memo: - Adding or updating documentation.
- :rocket: - Deploying stuff.
- :lipstick: - Updating the UI and style files.
- :tada: - Beginning a project.

Example of use:
`:wrench: add husky and commitlint config`

## Branching name

- `hotfix/`: for quickly fixing critical issues,
- `usually/`: with a temporary solution
- `bugfix/`: for fixing a bug
- `feature/`: for adding, removing or modifying a feature
- `test/`: for experimenting something which is not an issue
- `wip/`: for a work in progress

And add the issue id after an `/` followed with an explanation of the task.

Example of use:
`feature/5348-create-react-app`
