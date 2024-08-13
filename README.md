# Important

I'm archiving this repository as I'm going to join effort on a more advanced version of a DuckDB BigQuery FDW extension:
https://github.com/hafenkran/duckdb-bigquery

---- 
# Duckdb_bigquery

⚠ **This extension is a work in progress and is very early stage.** ⚠

This extension is meant to be a foreign data wrapper for BigQuery.

## Features

- [x] Read from BigQuery tables (Storage API) -- currently only supports reading from tables, not views
- [x] Google Application Default Credentials (ADC) support
- [x] Service account JSON credentials support
- [x] Projection (column) pushdown
- [x] LIMIT / OFFSET pushdown
- [x] Filter (WHERE) pushdown
- [ ] Write to BigQuery tables
- [ ] Support for BigQuery DDL
- [ ] Support for BigQuery DML

## Quickstart

For the time being, you'll need to build the extension yourself. To do so, follow the instructions in the [Building](##building) section.

To run the extension code, simply start the shell with `./build/release/duckdb -unsigned`.
 
Once you have the extension built, you can load it in DuckDB and attach a BigQuery project like so:

```sql
LOAD 'build/release/extension/duckdb_bigquery/duckdb_bigquery.duckdb_extension';

ATTACH 'my_gcp_bq_storage_project' AS bq (TYPE duckdb_bigquery);
```

You can then query your BigQuery tables like so:

```sql
SELECT my_column FROM bq.my_dataset.my_table;
```

```
┌───────────────┐
│    my_column  │
│    varchar    │
├───────────────┤
│ My bq data!   │
└───────────────┘
```

## Authentication

### Google Application Default Credentials (ADC)

The extension uses Google Application Default Credentials (ADC) to authenticate with BigQuery. This means that you need to have the `GOOGLE_APPLICATION_CREDENTIALS` environment variable set to the path of your service account key file. You can set this environment variable like so:

```shell
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/service-account-key.json"
```

For more details about setting up ADC, see the [Google Cloud documentation](https://cloud.google.com/docs/authentication/gcloud).

### Service account JSON credentials

It requires to create a secret with the service account credentials like the following:
```
CREATE SECRET duckdb_bigquery_secret (
    TYPE bigquery,
    service_account_json '{ "type": "service_account", "project_id": "my-gcp-project", "private_key_id": "xxxx", "private_key": "-----BEGIN PRIVATE KEY-----\nxxx\n-----END PRIVATE KEY-----\n", "client_email": "xxx@some-gcp-project.iam.gserviceaccount.com", "client_id": "xxx", "auth_uri": "https://accounts.google.com/o/oauth2/auth", "token_uri": "https://oauth2.googleapis.com/token", "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs", "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/xxx" }'
);

ATTACH 'my_gcp_bq_storage_project' AS bq (TYPE duckdb_bigquery, SECRET duckdb_bigquery_secret);

SELECT *
FROM bq.my_dataset.my_table
LIMIT 10;
```

## Configuration

### Changing execution project

By default, the extension will use the storage project for execution. If you want to use a different project for execution, you can specify it in the `ATTACH` statement like so:

```sql
ATTACH 'my_gcp_bq_storage_project' AS bq (TYPE duckdb_bigquery, EXECUTION_PROJECT 'my_gcp_bq_execution_project');
```

### Disabling filter pushdown

By default, the extension will push down filters to BigQuery. If you want to disable this, you can specify it by setting an option:

```sql
  SET bigquery_filter_pushdown=false;

```

## Building
### Managing dependencies
DuckDB extensions uses VCPKG for dependency management. Enabling VCPKG is very simple: follow the [installation instructions](https://vcpkg.io/en/getting-started) or just run the following:
```shell
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```
Note: VCPKG is only required for extensions that want to rely on it for dependency management. If you want to develop an extension without dependencies, or want to do your own dependency management, just skip this step. Note that the example extension uses VCPKG to build with a dependency for instructive purposes, so when skipping this step the build may not work without removing the dependency.

### Build steps
Now to build the extension, run:
```sh
VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug
```
The main binaries that will be built are:
```sh
./build/release/duckdb
./build/release/test/unittest
./build/release/extension/duckdb_bigquery/duckdb_bigquery.duckdb_extension
```
- `duckdb` is the binary for the duckdb shell with the extension code automatically loaded.
- `unittest` is the test runner of duckdb. Again, the extension is already linked into the binary.
- `duckdb_bigquery.duckdb_extension` is the loadable binary as it would be distributed.


## Running the tests
Different tests can be created for DuckDB extensions. The primary way of testing DuckDB extensions should be the SQL tests in `./test/sql`. These SQL tests can be run using:
```sh
make test
```

### Installing the deployed binaries
To install your extension binaries from S3, you will need to do two things. Firstly, DuckDB should be launched with the
`allow_unsigned_extensions` option set to true. How to set this will depend on the client you're using. Some examples:

CLI:
```shell
duckdb -unsigned
```

Python:
```python
con = duckdb.connect(':memory:', config={'allow_unsigned_extensions' : 'true'})
```

NodeJS:
```js
db = new duckdb.Database(':memory:', {"allow_unsigned_extensions": "true"});
```

Secondly, you will need to set the repository endpoint in DuckDB to the HTTP url of your bucket + version of the extension
you want to install. To do this run the following SQL query in DuckDB:
```sql
SET custom_extension_repository='bucket.s3.eu-west-1.amazonaws.com/<your_extension_name>/latest';
```
Note that the `/latest` path will allow you to install the latest extension version available for your current version of
DuckDB. To specify a specific version, you can pass the version instead.

After running these steps, you can install and load your extension using the regular INSTALL/LOAD commands in DuckDB:
```sql
INSTALL duckdb_bigquery
LOAD duckdb_bigquery
```
