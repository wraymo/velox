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

#include "velox/dwio/text/reader/TextReader.h"
#include "velox/dwio/text/common/Common.h"


#include <utility>
#include "velox/common/base/Pointers.h"
#include "velox/common/encode/Base64.h"
#include "velox/dwio/common/TypeWithId.h"

namespace facebook::velox::text {
TextReader::TextReader(
    std::unique_ptr<dwio::common::BufferedInput> input,
    const dwio::common::ReaderOptions& options)
    : schema_(options.fileSchema()),
  input_(std::move(input)),
      options_(options),
      typeWithId_(std::shared_ptr<const dwio::common::TypeWithId>(
          dwio::common::TypeWithId::create(schema_))) {
}

std::optional<uint64_t> TextReader::numberOfRows() const {
  return std::nullopt;
}

std::unique_ptr<dwio::common::ColumnStatistics> TextReader::columnStatistics(
      uint32_t index) const {
  return nullptr;
}

const RowTypePtr& TextReader::rowType() const {
  return schema_;
};


std::unique_ptr<dwio::common::RowReader> TextReader::createRowReader(
    const dwio::common::RowReaderOptions& /*options*/) const {
  return std::make_unique<TextRowReader>(schema_, options_, input_);
}

const std::shared_ptr<const dwio::common::TypeWithId>& TextReader::typeWithId()
    const {
  return typeWithId_;
}

TextRowReader::TextRowReader(
    const RowTypePtr& schema,
    const dwio::common::ReaderOptions& options,
    std::unique_ptr<dwio::common::BufferedInput> input)
    : schema_(schema),
      input_(std::move(input)),
      options_(options),
      memoryPool_(&options.memoryPool()) {
  fileLength_ = input_->getReadFile()->size();
  fileOffset_ = 0;
  blockOffset_ = 0;
  blockEndOffset_ = 0;

  bufferPtr_ = nullptr;
}

uint64_t TextRowReader::next(
    uint64_t size,
    VectorPtr& result,
    const dwio::common::Mutation* /*mutation*/) {
  if (!result) {
    result = BaseVector::create(schema_, size, memoryPool_);
  } else {
    VELOX_CHECK(
        result->type()->equivalent(*schema_),
        "Result vector type does not match the expected schema.");
  }

  auto rowResult = result->as<RowVector>();

  // Read the rows from the file and populate the column vectors.
  uint64_t row = 0;
  while (row < size) {
    // Load new block if needed
    if (!reader_ || fileOffset_ == blockEndOffset_) {
      if (fileOffset_ >= fileLength_) {
        break; // EOF
      }

      auto readSize = std::min(kBlockSize, fileLength_ - fileOffset_);
      reader_ = input_->enqueue({fileOffset_, readSize});
      input_->load(dwio::common::LogType::BLOCK);
      blockEndOffset_ = fileOffset_ + readSize;

      // Reset buffer state
      bufferPtr_ = nullptr;
      bufferSize_ = 0;
      bufferOffset_ = 0;
    }

    // Read buffer if fully consumed
    if (bufferOffset_ >= bufferSize_) {
      if (!reader_->Next(reinterpret_cast<const void**>(&bufferPtr_), &bufferSize_)) {
        continue; // No more buffers
      }
      bufferOffset_ = 0;
    }

    // Parse lines from current buffer
    while (bufferOffset_ < bufferSize_ && row < size) {
      const char* startPtr = bufferPtr_ + bufferOffset_;

      auto remainingStr = std::string_view(startPtr, bufferSize_ - bufferOffset_);
      auto find = remainingStr.find('\n');
      if (find == std::string::npos) {
        leftover_.append(startPtr, bufferSize_ - bufferOffset_);
        bufferOffset_ = bufferSize_;
        break;
      }

      if (!leftover_.empty()) {
        leftover_.append(startPtr, find);
        processLine(result, row, leftover_);
        leftover_.clear();
      } else {
        processLine(result, row, std::string_view(startPtr, find));
      }

      ++row;
      bufferOffset_ += lineLen + 1; // move past '\n'
    }
  }

  while (true) {
    // Load the next block if needed
    if (fileOffset_ == blockEndOffset_) {
      if (fileOffset_ >= fileLength_) {
        break; // End of file
      }
      auto readSize = std::min(kBlockSize, fileLength_ - fileOffset_);
      reader_ = input_->enqueue({fileOffset_, readSize});
      input_->load(dwio::common::LogType::BLOCK);
      blockEndOffset_ = fileOffset_ + readSize;
      fileOffset_ = blockEndOffset_;
    }

    while (reader_->Next(&data_, &dataSize_)) {

    }


  }


  for (; row < size; ++row) {
    for (uint64_t i = 0; i < schema_->size(); ++i) {
      // Check if the column is null and handle it accordingly.
      if (rowResult->childAt(i)->isNullAt(row)) {
        continue;
      }
    }
    if (!readRow(result, row)) {
      break; // No more rows to read.
    }
  }

}

void TextRowReader::processLine(VectorPtr& result, int32_t row, std::string_view line) {
  std::vector<std::string> result;
  std::size_t start = 0;
  std::size_t end = line.find(TextFileTraits::kSOH);

  while (end != std::string::npos) {
    result.push_back(str.substr(start, end - start));
    start = end + 1;
    end = str.find(delimiter, start);
  }

  result.push_back(str.substr(start)); // Add the last part
  return result;
}

// Helper function to read a single value from the current line.
StringView TextRowReader::readValue() {
  // Skip any leading whitespace.  In a real implementation, you might
  // want to make this configurable.
  while (linePos_ < lineBuffer_->size() &&
         std::isspace(lineBuffer_->as<char>()[linePos_])) {
    ++linePos_;
  }

  // Find the end of the value.  The value is terminated by either
  // the column separator or the end of the line.
  int32_t start = linePos_;
  while (linePos_ < lineBuffer_->size() &&
         lineBuffer_->as<char>()[linePos_] != TextFileTraits::kSOH &&
         lineBuffer_->as<char>()[linePos_] != TextFileTraits::kNewLine) {
    ++linePos_;
  }
  int32_t end = linePos_;

  // Skip the column separator, if present.
  if (linePos_ < lineBuffer_->size() &&
      lineBuffer_->as<char>()[linePos_] == TextFileTraits::kSOH) {
    ++linePos_;
  }

  // Return the value as a StringView.
  return StringView(lineBuffer_->as<char>() + start, end - start);
}

// Helper function to read a row from the input stream.
bool readRow(RowVectorPtr& result, int32_t row) {
  // Read a line from the input stream.
  if (!readLine(result, row)) {
    return false; // No more rows to read.
  }

  // Read the values from the line and populate the column vectors.
  for (int32_t column = 0; column < columnCount_; ++column) {
    StringView value = readValue();
    if (value == TextFileTraits::kNullData) {
      result->childAt(column)->setNull(row, true);
    } else {
      writeColumnValue(result->childAt(column), row, value);
    }
  }
  return true;
}

// Helper function to write a value to a column vector.
void TextRowReader::writeColumnValue(
    VectorPtr& columnVector,
    int32_t row,
    const StringView& value) {
  switch (columnVector->type()->kind()) {
    case TypeKind::BOOLEAN:
      columnVector->as<FlatVector<bool>>()->set(row, stringViewToBool(value));
      break;
    case TypeKind::TINYINT:
      columnVector->as<FlatVector<int8_t>>()->set(
          row, stringViewToInteger<int8_t>(value));
      break;
    case TypeKind::SMALLINT:
      columnVector->as<FlatVector<int16_t>>()->set(
          row, stringViewToInteger<int16_t>(value));
      break;
    case TypeKind::INTEGER:
      columnVector->as<FlatVector<int32_t>>()->set(
          row, stringViewToInteger<int32_t>(value));
      break;
    case TypeKind::BIGINT:
      columnVector->as<FlatVector<int64_t>>()->set(
          row, stringViewToInteger<int64_t>(value));
      break;
    case TypeKind::REAL:
      columnVector->as<FlatVector<float>>()->set(
          row, stringViewToFloat<float>(value));
      break;
    case TypeKind::DOUBLE:
      columnVector->as<FlatVector<double>>()->set(
          row, stringViewToFloat<double>(value));
      break;
    case TypeKind::VARCHAR: {
      // Create a copy of the string in the vector's memory pool.
      columnVector->as<FlatVector<StringView>>()->set(
          row, StringView(value.data(), value.size()));
      break;
    }
    case TypeKind::TIMESTAMP:
      columnVector->as<FlatVector<Timestamp>>()->set(
          row, stringViewToTimestamp(value));
      break;
    default:
      VELOX_NYI("Unsupported type: {}", columnVector->type()->toString());
  }
}

// Helper function definitions
namespace {
bool stringViewToBool(const StringView& sv) {
  if (sv.size() == 4 && folly::ascii_strncasecmp(sv.data(), "true", 4) == 0) {
    return true;
  }
  if (sv.size() == 5 && folly::ascii_strncasecmp(sv.data(), "false", 5) == 0) {
    return false;
  }
  // try to convert it to a number. If it is not zero, return true, otherwise
  // false.
  try {
    int64_t numValue = stringViewToInteger<int64_t>(sv);
    return numValue != 0;
  } catch (const std::invalid_argument& e) {
    throw std::invalid_argument(
        "Invalid boolean value: " + std::string(sv.data(), sv.size()));
  }
}

template <typename T>
T stringViewToInteger(const StringView& sv) {
  if (sv.empty()) {
    throw std::invalid_argument(
        "Empty string cannot be converted to an integer");
  }
  const char* start = sv.data();
  const char* end = sv.data() + sv.size();
  T value = 0;
  bool negative = false;

  if (*start == '-') {
    negative = true;
    ++start;
  } else if (*start == '+') {
    ++start;
  }

  if (start >= end) {
    throw std::invalid_argument("Invalid integer format: only sign present");
  }
  for (const char* ptr = start; ptr < end; ++ptr) {
    if (!std::isdigit(*ptr)) {
      throw std::invalid_argument(
          "Invalid integer value: " + std::string(sv.data(), sv.size()));
    }
    T digit = *ptr - '0';
    if (value > (std::numeric_limits<T>::max() - digit) / 10) {
      throw std::overflow_error("Integer overflow");
    }
    value = value * 10 + digit;
  }
  return negative ? -value : value;
}

template <typename T>
T stringViewToFloat(const StringView& sv) {
  if (sv.empty()) {
    throw std::invalid_argument("Empty string cannot be converted to a float");
  }
  const char* start = sv.data();
  const char* end = sv.data() + sv.size();

  // Use std::from_chars for string to float conversion
  T value;
  auto result = std::from_chars(start, end, value);

  if (result.ec != std::errc()) {
    if (sv.size() == 3 && folly::ascii_strncasecmp(sv.data(), "NaN", 3) == 0)
      return std::numeric_limits<T>::quiet_NaN();
    if (sv.size() == 8 &&
        folly::ascii_strncasecmp(sv.data(), "Infinity", 8) == 0)
      return std::numeric_limits<T>::infinity();
    if (sv.size() == 9 &&
        folly::ascii_strncasecmp(sv.data(), "-Infinity", 9) == 0)
      return -std::numeric_limits<T>::infinity();
    throw std::invalid_argument(
        "Invalid float value: " + std::string(sv.data(), sv.size()) +
        " error code: " + std::to_string(static_cast<int>(result.ec)));
  }
  return value;
}

Timestamp stringViewToTimestamp(const StringView& sv) {
  // Attempt to parse the timestamp using boost::date_time.
  std::string timestampStr(sv.data(), sv.size());
  try {
    return Timestamp::fromISOString(timestampStr);
  } catch (const std::runtime_error& e) {
    throw std::invalid_argument(
        "Invalid timestamp value: " + std::string(sv.data(), sv.size()));
  }
}
} // namespace
} // namespace facebook::velox::text
