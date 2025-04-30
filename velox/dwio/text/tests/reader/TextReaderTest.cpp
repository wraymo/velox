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
#include "velox/dwio/text/writer/TextWriter.h"

#include <gtest/gtest.h>
#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/dwio/common/tests/utils/DataFiles.h"
#include "velox/dwio/text/tests/writer/FileReaderUtil.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"

namespace facebook::velox::text {

class TextReaderTest : public testing::Test,
                       public velox::test::VectorTestBase {
 public:
  void SetUp() override {
    velox::filesystems::registerLocalFileSystem();
    rootPool_ = memory::memoryManager()->addRootPool("TextReaderTests");
    leafPool_ = rootPool_->addLeafChild("TextReaderTests");
  }

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  std::string getExampleFilePath(const std::string& fileName) {
    return test::getDataFilePath(
        "velox/dwio/text/tests/reader", "../examples/" + fileName);
  }

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const std::string& fileName,
      const RowTypePtr& schema) {
    const std::string sample(getExampleFilePath(fileName));

    dwio::common::ReaderOptions readerOptions{leafPool_.get()};
    readerOptions.setFileFormat(dwio::common::FileFormat::TEXT);

    readerOptions.setFileSchema(schema);
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>(sample), readerOptions.memoryPool());
    auto reader = std::make_unique<facebook::velox::text::TextReader>(
        std::move(input), std::move(readerOptions));

    dwio::common::RowReaderOptions rowReaderOpts;
    auto rowReader = reader->createRowReader(rowReaderOpts);
    return rowReader;
  }

  constexpr static float kInf = std::numeric_limits<float>::infinity();
  constexpr static double kNaN = std::numeric_limits<double>::quiet_NaN();
  std::shared_ptr<memory::MemoryPool> rootPool_;
  std::shared_ptr<memory::MemoryPool> leafPool_;
};

TEST_F(TextReaderTest, read) {
  auto schema =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
          {BOOLEAN(),
           TINYINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           TIMESTAMP(),
           VARCHAR(),
           VARBINARY()});

  auto rowReader = createRowReader("test_text_writer.txt", schema);

  auto expected = makeRowVector(
      {"c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9"},
      {
          makeConstant(true, 3),
          makeFlatVector<int8_t>({1, 2, 3}),
          makeFlatVector<int16_t>({1, 2, 3}), // TODO null
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int64_t>({1, 2, 3}),
          makeFlatVector<float>({1.1, kInf, 3.1}),
          makeFlatVector<double>({1.1, kNaN, 3.1}),
          makeFlatVector<Timestamp>(
              3, [](auto i) { return Timestamp(i, i * 1'000'000); }),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARCHAR()),
          makeFlatVector<StringView>({"hello", "world", "cpp"}, VARBINARY()),
      });

  VectorPtr actual;
  auto result = rowReader->next(3, actual);
  ASSERT_EQ(result, 3);
  ASSERT_EQ(actual->size(), 3);
  ASSERT_EQ(actual->type(), schema);
  test::assertEqualVectors(actual, expected);
}
} // namespace facebook::velox::text
