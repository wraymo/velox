#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include "velox/connectors/clp/ClpColumnHandle.h"
#include "velox/connectors/clp/ClpConnectorSplit.h"
#include "velox/connectors/clp/ClpDataSource.h"
#include "velox/connectors/clp/ClpTableHandle.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::connector::clp {

ClpDataSource::ClpDataSource(
    const RowTypePtr& outputType,
    const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
    const std::unordered_map<
        std::string,
        std::shared_ptr<connector::ColumnHandle>>& columnHandles,
    velox::memory::MemoryPool* pool,
    std::shared_ptr<const ClpConfig>& clpConfig)
    : pool_(pool), outputType_(outputType) {
  executablePath_ = clpConfig->executablePath();
  VELOX_CHECK(!executablePath_.empty(), "Executable path must be set");
  polymorphicTypeEnabled_ = clpConfig->polymorphicTypeEnabled();
  auto archiveRootDir = clpConfig->archiveDir();
  VELOX_CHECK(!archiveRootDir.empty(), "Archive directory must be set");
  auto clpTableHandle = std::dynamic_pointer_cast<ClpTableHandle>(tableHandle);
  auto archiveDir =
      boost::filesystem::path(archiveRootDir) / clpTableHandle->tableName();
  archiveDir_ = archiveDir.string();
  auto query = clpTableHandle->query();
  if (query && !query->empty()) {
    kqlQuery_ = *query;
  } else {
    kqlQuery_ = "*";
  }

  auto outputNames = outputType->names();
  for (size_t i = 0; i < outputNames.size(); ++i) {
    auto columnHandle = columnHandles.find(outputNames[i]);
    VELOX_CHECK(
        columnHandle != columnHandles.end(),
        "ColumnHandle not found for output name: {}",
        outputNames[i]);
    auto clpColumnHandle =
        std::dynamic_pointer_cast<ClpColumnHandle>(columnHandle->second);
    VELOX_CHECK_NOT_NULL(
        clpColumnHandle,
        "ColumnHandle must be an instance of ClpColumnHandle for output name: {}",
        outputNames[i]);
    auto columnName = clpColumnHandle->columnName();
    columnIndices_[columnName] = i;
    if (polymorphicTypeEnabled_) {
      static const std::vector<std::string> suffixes = {
          "_varchar", "_double", "_bigint", "_boolean"};
      bool suffixFound = false;

      for (const auto& suffix : suffixes) {
        if (boost::algorithm::ends_with(columnName, suffix)) {
          // Strip the type suffix
          columnUntypedNames_.insert(
              columnName.substr(0, columnName.size() - suffix.size()));
          suffixFound = true;
          break;
        }
      }

      if (!suffixFound) {
        columnUntypedNames_.insert(columnName);
      }
    } else {
      columnUntypedNames_.insert(columnName);
    }
  }
}

ClpDataSource::~ClpDataSource() {
  if (process_.running()) {
    process_.terminate();
  }
}

void ClpDataSource::addSplit(std::shared_ptr<ConnectorSplit> split) {
  auto clpSplit = std::dynamic_pointer_cast<ClpConnectorSplit>(split);
  auto tableName = clpSplit->tableName();
  auto archiveId = clpSplit->archiveId();
  VELOX_CHECK(!tableName.empty(), "Table name must be set");
  std::vector<std::string> commands = {
      "s", archiveDir_, "--archive-id", archiveId, kqlQuery_};
  if (!columnUntypedNames_.empty()) {
    commands.emplace_back("--projection");
    commands.insert(
        commands.end(), columnUntypedNames_.begin(), columnUntypedNames_.end());
  }
  resultsStream_ = boost::process::ipstream();
  arrayOffsets_.clear();
  if (process_.running()) {
    process_.terminate();
  }
  process_ = boost::process::child(
      executablePath_, commands, boost::process::std_out > resultsStream_);
}

std::optional<RowVectorPtr> ClpDataSource::next(
    uint64_t size,
    ContinueFuture& future) {
  std::vector<VectorPtr> vectors;
  vectors.reserve(outputType_->size());
  auto nulls = AlignedBuffer::allocate<bool>(size, pool_, bits::kNull);
  for (const auto& childType : outputType_->children()) {
    // Create a vector with NULL values
    auto vector = BaseVector::create(childType, size, pool_);
    vector->setNulls(nulls);
    vectors.emplace_back(vector);
  }

  uint64_t localCompletedRows = 0;
  for (uint64_t i = 0; i < size; ++i) {
    std::string line;
    if (std::getline(resultsStream_, line)) {
      localCompletedRows++;
      completedBytes_ += line.size();
      if (0 == outputType_->size()) {
        continue;
      }
      // Parse the line and return the RowVectorPtr
      simdjson::ondemand::parser parser;
      auto doc = parser.iterate(line);
      std::string path;
      parseJsonLine(doc, path, vectors, i);
    } else {
      // No more data to read
      if (process_.running()) {
        process_.terminate();
      }
      break;
    }
  }
  if (localCompletedRows == 0) {
    return nullptr;
  }
  completedRows_ += localCompletedRows;
  return std::make_shared<RowVector>(
      pool_, outputType_, BufferPtr(), localCompletedRows, std::move(vectors));
}

void ClpDataSource::parseJsonLine(
    simdjson::ondemand::value element,
    std::string& path,
    std::vector<VectorPtr>& vectors,
    uint64_t index) {
  // Parse the json element and populate the vectors
  switch (element.type()) {
    case simdjson::ondemand::json_type::object:
      for (auto field : element.get_object()) {
        std::string_view key = field.unescaped_key();
        std::string newPath =
            path.empty() ? std::string(key) : path + "." + std::string(key);
        parseJsonLine(field.value(), newPath, vectors, index);
      }
      break;
    case simdjson::ondemand::json_type::string: {
      setValue(
          vectors,
          path,
          index,
          StringView(element.get_string().value()),
          "varchar");
      break;
    }
    case simdjson::ondemand::json_type::number: {
      simdjson::ondemand::number elementNumber = element.get_number();
      if (elementNumber.is_double()) {
        setValue(vectors, path, index, elementNumber.get_double(), "double");
      } else {
        setValue(vectors, path, index, elementNumber.get_int64(), "bigint");
      }
      break;
    }
    case simdjson::ondemand::json_type::boolean: {
      setValue(vectors, path, index, element.get_bool().value(), "boolean");
      break;
    }
    case simdjson::ondemand::json_type::array: {
      std::shared_ptr<ArrayVector> arrayVector;
      if (auto iter = columnIndices_.find(path); iter != columnIndices_.end()) {
        arrayVector =
            std::dynamic_pointer_cast<ArrayVector>(vectors[iter->second]);
      } else if (polymorphicTypeEnabled_) {
        auto typedPath = path + "_varchar";
        if (iter = columnIndices_.find(typedPath);
            iter != columnIndices_.end()) {
          arrayVector =
              std::dynamic_pointer_cast<ArrayVector>(vectors[iter->second]);
        }
      } else {
        break;
      }
      if (arrayOffsets_.find(path) == arrayOffsets_.end()) {
        arrayOffsets_[path] = 0;
      }
      auto arrayBeginOffset = arrayOffsets_[path];
      auto arrayEndOffset = arrayBeginOffset;
      auto elements = arrayVector->elements()->asFlatVector<StringView>();
      std::vector<std::string_view> arrayElements;
      for (auto arrayElement : element.get_array()) {
        // Get each array element as a string
        auto elementStringWithQuotes =
            simdjson::to_json_string(arrayElement).value();
        auto elementString = elementStringWithQuotes.substr(
            1, elementStringWithQuotes.size() - 2);
        arrayElements.emplace_back(elementString);
      }
      elements->resize(arrayEndOffset + arrayElements.size());

      for (auto& arrayElement : arrayElements) {
        // Set the element in the array vector
        elements->set(arrayEndOffset++, StringView(arrayElement));
      }
      arrayOffsets_[path] = arrayEndOffset;
      arrayVector->setOffsetAndSize(
          index, arrayBeginOffset, arrayEndOffset - arrayBeginOffset);
      arrayVector->setNull(index, false);
      break;
    }
    case simdjson::ondemand::json_type::null:
      break;
  }
}
} // namespace facebook::velox::connector::clp
