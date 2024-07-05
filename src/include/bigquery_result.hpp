//===----------------------------------------------------------------------===//
//                         DuckDB
//
// bigquery_result.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdio>
#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include <arrow/api.h>

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

class BigQueryResult {
public:
	// string execution_project;
	// string storage_project;
	// string dataset;
	// string table;
	// std::shared_ptr<arrow::Schema> schema;
	// unique_ptr<bigquery_storage::BigQueryReadClient> client;
	// unique_ptr<bigquery_storage_read::ReadSession> read_session;

	// BigQueryResult(
	// 	string execution_project,
	// 	string storage_project,
	// 	string dataset,
	// 	string table,
	// 	std::shared_ptr<arrow::Schema> schema_p,
	// 	unique_ptr<bigquery_storage::BigQueryReadClient> client_p,
	// 	unique_ptr<bigquery_storage_read::ReadSession> read_session_p
	// 	):
	// 	  execution_project(execution_project),
	// 	  storage_project(storage_project),
	// 	  dataset(dataset),
	// 	  table(table),
	// 	  schema(std::move(schema_p)),
	// 	  client(std::move(client_p)),
	// 	  read_session(std::move(read_session_p)),
	// 	  current_offset(0)
	// 	  {}

	~BigQueryResult() {}

public:
	static std::shared_ptr<arrow::RecordBatch> GetArrowRecordBatch(
    ::bigquery_storage_read::ArrowRecordBatch const&
        record_batch_in,
    std::shared_ptr<arrow::Schema> schema);
private:
	idx_t current_offset = 0;

};

} // namespace duckdb
