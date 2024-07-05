#include "duckdb.hpp"

#include "google/cloud/bigquery/storage/v1/bigquery_read_client.h"
#include <arrow/api.h>
#include <arrow/array/data.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include "bigquery_result.hpp"

namespace bigquery_storage = ::google::cloud::bigquery_storage_v1;
namespace bigquery_storage_read = ::google::cloud::bigquery::storage::v1;

namespace duckdb {

std::shared_ptr<arrow::RecordBatch> BigQueryResult::GetArrowRecordBatch(
    ::google::cloud::bigquery::storage::v1::ArrowRecordBatch const&
        record_batch_in,
    std::shared_ptr<arrow::Schema> schema) {
  std::shared_ptr<arrow::Buffer> buffer = std::make_shared<arrow::Buffer>(
      record_batch_in.serialized_record_batch());
  arrow::io::BufferReader buffer_reader(buffer);
  arrow::ipc::DictionaryMemo dictionary_memo;
  arrow::ipc::IpcReadOptions read_options;
  auto result = arrow::ipc::ReadRecordBatch(schema, &dictionary_memo,
                                            read_options, &buffer_reader);
  if (!result.ok()) {
    Printer::Print("Unable to parse record batch:" + result.status().message());
    throw result.status();
  }
  std::shared_ptr<arrow::RecordBatch> record_batch = result.ValueOrDie();
  return record_batch;
}

} // namespace duckdb
