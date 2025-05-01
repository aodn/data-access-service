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

You might want to update the `poetry.lock` file after manually modifying `pyproject.toml` with `poetry lock` command. To update all dependencies, use `poetry update` command.

There is a library `aodn_cloud_optimised`, which referencing direct AODN's GitHub repository as source instead of `PyPi`. It is written by PO to access
cloud optimized data. Please do not access S3 directly and should via this library.

Right now the version is always `main` branch which is not idea, we should be able to use tag version

```bash
aodn_cloud_optimised = { git = "https://github.com/aodn/aodn_cloud_optimised.git", branch = "main" }
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
   ```bash
   # You should not need to install lib locally, if your python version is correct.
   # https://arrow.apache.org/install/
   sudo apt update
   sudo apt install -y -V ca-certificates lsb-release wget
   wget https://apache.jfrog.io/artifactory/arrow/$(lsb_release --id --short | tr 'A-Z' 'a-z')/apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   sudo apt install -y -V ./apache-arrow-apt-source-latest-$(lsb_release --codename --short).deb
   sudo apt update
   sudo apt install -y -V libarrow-dev # For C++
   sudo apt install -y -V libarrow-glib-dev # For GLib (C)
   sudo apt install -y -V libarrow-dataset-dev # For Apache Arrow Dataset C++
   sudo apt install -y -V libarrow-dataset-glib-dev # For Apache Arrow Dataset GLib (C)
   sudo apt install -y -V libarrow-acero-dev # For Apache Arrow Acero
   sudo apt install -y -V libarrow-flight-dev # For Apache Arrow Flight C++
   sudo apt install -y -V libarrow-flight-glib-dev # For Apache Arrow Flight GLib (C)
   sudo apt install -y -V libarrow-flight-sql-dev # For Apache Arrow Flight SQL C++
   sudo apt install -y -V libarrow-flight-sql-glib-dev # For Apache Arrow Flight SQL GLib (C)
   sudo apt install -y -V libgandiva-dev # For Gandiva C++
   sudo apt install -y -V libgandiva-glib-dev # For Gandiva GLib (C)
   sudo apt install -y -V libparquet-dev # For Apache Parquet C++
   sudo apt install -y -V libparquet-glib-dev # For Apache Parquet GLib (C)
   sudo apt install -y ninja-build
    ```

3. **Run the app:**
    In project root folder, create a '.env' file, which contains your API key, e.g.:
    ```
    API_KEY="your_actual_api_key_here"
    ```

   Host will be `http://localhost:8000`.

    ```bash
    $ poetry run uvicorn data_access_service.server:app --reload --log-config=log_config.yaml
    ```

### Code formatting

The command below is for manual checks; checks are also executed when you run `git commit`.

The configurations for pre-commit hooks are defined in `.pre-commit-config.yaml`.

```shell
pre-commit run --all-files
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

| Description        | Endpoints                          | Param                                                                                                                                                                                       | Environment                                                                                                                                           |
|--------------------|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------|
| Formatted metadata | /api/v1/das/metadata/{uuid}  | none                                                                                                                                                                                        | ALL                                                                                                                                                   |
| Raw metadata       | /api/v1/das/metadata/{uuid}/raw | none                                                                                                                                                                                        | ALL                                                                                                                                                   |
| Subsetting         | /api/v1/das/data/{uuid} | start_date=2023-12-25T14:30:00, end_date=2024-02-25T14:30:00, start_depth=-0.06, f=netcdf or json, columns=TIME&columns=DEPTH&columns=LONGITUDE&columns=LATITUDE (array of column return), is_to_index=true | ALL |


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
./data_access_service/entrypoint.py
```

- If you have error like: "mount/efs permission denied" or other errors about the mount/efs folder, please go to `generate_csv_file.py`,
at the top of the file, make sure `efs_mount_point ="" ` is in use and `efs_mount_point = "/mount/efs/"` is commented out.
This is just a temp solution. We will then use source file to manage these different values in different environments.

### aws running testing
- If you want to test the batch job codes in AWS, (running in AWS Batch), please change the `efs_mount_point ="" ` back to `efs_mount_point = "/mount/efs/"` (if you did the change before), and then:
  1. Build the docker image and push it to ECR (havier-test-ecr). Please do it by following the instructions in the havier-test-ecr repo by clicking button "View push commands" at the top right corner.
  2. Open the ogc-api project locally
  3. In class DatasetDownloadEnums of the ogc-api project, go to JobDefinition enum and change the value of GENERATE_CSV_DATA_FILE from "generate-csv-data-file" into "generate-csv-data-file-dev" (add "-dev" at the end of the string).
  4. run the ogc-api project locally and run the portal project locally (make sure your local portal is pointing to the local ogc-api project).
  5. Go to localhost:5173, and navigate to an IMOS hosted dataset. On the detail page, select any date range and / or spatial area, and click "Download" button. This will trigger the batch job.
  6. For dev stage, if you cannot receive the email, please make sure the email address you provided has added into the AWS SES verified email list.
