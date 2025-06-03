/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <utility>

#include "velox/common/encode/Base64.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/dwio/text/common/Common.h"
#include "velox/dwio/text/reader/TextReader.h"

namespace facebook::velox::text {
ReaderBase::ReaderBase(
    dwio::common::ReaderOptions options,
    std::unique_ptr<dwio::common::BufferedInput> input)
    : options_{std::move(options)},
      input_{std::move(input)},
      schema_{options_.fileSchema()},
      typeWithId_{std::shared_ptr<const dwio::common::TypeWithId>(
          dwio::common::TypeWithId::create(schema_))},
      memoryPool_(&options_.memoryPool()) {}

void ReaderBase::createVector(RowTypePtr& type, VectorPtr& result, vector_size_t size) const {
  if (!result) {
    result = BaseVector::create(type, size, memoryPool_);
  } else {
    VELOX_CHECK(
        result->type()->equivalent(*type),
        "Result vector type does not match the expected schema.");
    result->resize(size);
  }
}

std::unique_ptr<dwio::common::SeekableInputStream> ReaderBase::loadBlock(
    common::Region region) const {
  auto stream = input_->enqueue(region);
  input_->load(dwio::common::LogType::BLOCK);
  return stream;
}

TextRowReader::TextRowReader(
    const std::shared_ptr<ReaderBase>& reader,
    const dwio::common::RowReaderOptions& options)
    : readerBase_(reader),
      requestedType_{options.requestedType() ? options.requestedType()
                           : readerBase_->schema()},
      fileSchema_{readerBase_->schema()},
      fieldDelim_{readerBase_->serdeOptions().separators[0]},
      row_{0},
      fileLength_{readerBase_->fileLength()},
      fileOffset_{0},
      blockEndOffset_{0},
      bufferPtr_{nullptr},
      bufferSize_{0},
      bufferOffset_{0} {
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  auto& scanSpec = options.scanSpec();
  auto& childSpecs = scanSpec->children();
  for (auto i = 0; i < childSpecs.size(); ++i) {
    auto childSpec = childSpecs[i];
    if (!childSpec->readFromFile()) {
      continue;
    }
    auto index = fileSchema_->getChildIdx(childSpec->fieldName());
    fileIndexToOutputIndex_[index] = i;
    auto childRequestedType =
        requestedType_->asRow().findChild(childSpec->fieldName());
    names.push_back(childSpec->fieldName());
    types.push_back(childRequestedType);
  }
  outputType_ = ROW(std::move(names), std::move(types));
}

uint64_t TextRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* /*mutation*/) {
  readerBase_->createVector(outputType_, result, size);
  auto rowResult = result->as<RowVector>();

  int32_t row = 0;
  while (row < size) {
    // Load new block if needed
    if (fileOffset_ == blockEndOffset_) {
      if (fileOffset_ >= fileLength_) {
        break; // EOF
      }

      auto readSize = std::min(kBlockSize, fileLength_ - fileOffset_);
      stream_ = readerBase_->loadBlock({fileOffset_, readSize});
      blockEndOffset_ = fileOffset_ + readSize;

      // Reset buffer state
      bufferPtr_ = nullptr;
      bufferSize_ = 0;
      bufferOffset_ = 0;
    }

    // Read buffer if fully consumed
    if (bufferOffset_ >= bufferSize_) {
      if (!stream_->Next(
              reinterpret_cast<const void**>(&bufferPtr_), &bufferSize_)) {
        break;
      }
      bufferOffset_ = 0;
    }

    // Parse lines from current buffer
    std::string_view remainingStr(
        bufferPtr_ + bufferOffset_, bufferSize_ - bufferOffset_);
    while (!remainingStr.empty() && row < size) {
      auto end = remainingStr.find(TextFileTraits::kNewLine);
      if (end == std::string::npos) {
        leftover_.append(remainingStr);
        remainingStr = std::string_view();
        break;
      }

      if (!leftover_.empty()) {
        leftover_.append(remainingStr, 0, end);
        processLine(rowResult, row, leftover_);
        leftover_.clear();
      } else {
        processLine(rowResult, row, remainingStr.substr(0, end));
      }

      remainingStr.remove_prefix(end + 1);
      ++row;
    }

    bufferOffset_ = bufferSize_ - remainingStr.size();
    if (bufferOffset_ >= bufferSize_) {
      fileOffset_ += bufferSize_;
    }
  }

  result->resize(row);
  row_ += row;
  return row;
}

int64_t TextRowReader::nextReadSize(uint64_t size) {
  if (fileOffset_ >= fileLength_) {
    return kAtEnd;
  } else {
    return 0;
  }
}

void TextRowReader::processLine(
    RowVector* result,
    int32_t row,
    std::string_view line) {
  std::size_t columnIndex = 0;
  std::size_t start = 0;

  while (start <= line.size()) {
    VELOX_CHECK_LT(
        columnIndex, fileSchema_->size(), "Too many columns in line");

    std::size_t end = line.find(fieldDelim_, start);
    bool isLast = (end == std::string::npos);
    std::string_view token =
        isLast ? line.substr(start) : line.substr(start, end - start);
    auto it = fileIndexToOutputIndex_.find(columnIndex);
    if (it != fileIndexToOutputIndex_.end()) {
      writeColumnValue(result->childAt(it->second), row, token);
    }

    columnIndex++;
    if (isLast) {
      break;
    }
    start = end + 1;
  }
}

template <TypeKind KIND>
typename TypeTraits<KIND>::NativeType TextRowReader::castFromString(
    const std::string_view& value) {
  auto result = util::Converter<KIND>::tryCast(folly::StringPiece(value));
  VELOX_CHECK(!result.hasError());
  return result.value();
}

void TextRowReader::writeColumnValue(
    VectorPtr& columnVector,
    int32_t row,
    const std::string_view& value) {
  if (value == TextFileTraits::kNullData) {
    columnVector->setNull(row, true);
    return;
  }

  auto type = columnVector->type()->kind();
  switch (type) {
    case TypeKind::BOOLEAN:
      columnVector->as<FlatVector<bool>>()->set(
          row, castFromString<TypeKind::BOOLEAN>(value));
      break;
    case TypeKind::TINYINT:
      columnVector->as<FlatVector<int8_t>>()->set(
          row, castFromString<TypeKind::TINYINT>(value));
      break;
    case TypeKind::SMALLINT:
      columnVector->as<FlatVector<int16_t>>()->set(
          row, castFromString<TypeKind::SMALLINT>(value));
      break;
    case TypeKind::INTEGER:
      columnVector->as<FlatVector<int32_t>>()->set(
          row, castFromString<TypeKind::INTEGER>(value));
      break;
    case TypeKind::BIGINT:
      columnVector->as<FlatVector<int64_t>>()->set(
          row, castFromString<TypeKind::BIGINT>(value));
      break;
    case TypeKind::REAL:
      columnVector->as<FlatVector<float>>()->set(
          row, castFromString<TypeKind::REAL>(value));
      break;
    case TypeKind::DOUBLE:
      columnVector->as<FlatVector<double>>()->set(
          row, castFromString<TypeKind::DOUBLE>(value));
      break;
    case TypeKind::VARCHAR:
      columnVector->as<FlatVector<StringView>>()->set(
          row, StringView(value.data(), value.size()));
      break;
    case TypeKind::VARBINARY: {
      auto decodedValue =
          encoding::Base64::decode({value.data(), value.size()});
      columnVector->as<FlatVector<StringView>>()->set(
          row, StringView(decodedValue));
      break;
    }
    case TypeKind::TIMESTAMP:
      columnVector->as<FlatVector<Timestamp>>()->set(
          row, castFromString<TypeKind::TIMESTAMP>(value));
      break;
    default:
      VELOX_NYI("Unsupported type: {}", columnVector->type()->toString());
  }
}

TextReader::TextReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : readerBase_{std::make_shared<ReaderBase>(options, std::move(input))} {}
} // namespace facebook::velox::text
