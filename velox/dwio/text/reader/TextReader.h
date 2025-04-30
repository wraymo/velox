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

#pragma once

#include "velox/common/compression/Compression.h"
#include "velox/common/config/Config.h"
#include "velox/dwio/common/DataBuffer.h"
#include "velox/dwio/common/FileSink.h"
#include "velox/dwio/common/FlushPolicy.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::text {

class TextReader : public dwio::common::Reader {
 public:
  TextReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options);

  ~TextReader() override = default;

  std::optional<uint64_t> numberOfRows() const override;

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t index) const override;

  const RowTypePtr& rowType() const override;

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options) const override;

 private:
  const RowTypePtr& schema_;
  const std::shared_ptr<const dwio::common::TypeWithId> typeWithId_;
    std::unique_ptr<dwio::common::BufferedInput> input_;
    const dwio::common::ReaderOptions& options_;
};

class TextRowReader : public dwio::common::RowReader {
public:
    ~TextRowReader() override = default;
    TextRowReader(
    const RowTypePtr& schema,
    const dwio::common::ReaderOptions& options,
    std::unique_ptr<dwio::common::BufferedInput> input);
  uint64_t next(
      uint64_t size,
      VectorPtr& result,
      const dwio::common::Mutation* mutation) override;

  int64_t nextRowNumber() override;

  int64_t nextReadSize(uint64_t size) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override;

  void resetFilterCaches() override;

  std::optional<size_t> estimatedRowSize() const override;

    void processLine(VectorPtr& result, int32_t row, std::string_view line);

private:
    const dwio::common::ReaderOptions& options_;
    static constexpr uint64_t kBlockSize = 1024 * 1024;

    // Schema for the data being read.
    const RowTypePtr& schema_;

    // Input stream to read from.
    std::unique_ptr<dwio::common::BufferedInput> input_;

    // Memory pool.
    memory::MemoryPool* memoryPool_;

    std::unique_ptr<dwio::common::SeekableInputStream> reader_;

    uint64_t fileLength_;
    uint64_t fileOffset_;
    uint64_t blockOffset_;
    uint64_t blockEndOffset_;
    std::string leftover_;
    const char* bufferPtr_ = nullptr;
    int32_t bufferSize_ = 0;
    int32_t bufferOffset_ = 0;
};

class TextReaderFactory : public dwio::common::ReaderFactory {
 public:
  TextReaderFactory() : ReaderFactory(dwio::common::FileFormat::TEXT) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::BufferedInput> input,
      const dwio::common::ReaderOptions& options) override {
    return std::make_unique<TextReader>(std::move(input), options);
  }
};

} // namespace facebook::velox::text
