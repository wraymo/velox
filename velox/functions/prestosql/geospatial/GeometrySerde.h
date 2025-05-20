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

#include <geos/geom/Geometry.h>
#include "velox/type/StringView.h"

namespace facebook::velox::functions::geospatial {

class SliceInput {
 public:
  SliceInput(const void* rawData, size_t dataSize)
      : data_(reinterpret_cast<const uint8_t*>(rawData)),
        size_(dataSize),
        position_(0) {}

  uint8_t readByte() {
    return read<uint8_t>();
  }

  int16_t readShort() {
    return read<int16_t>();
  }

  int32_t readInt() {
    return read<int32_t>();
  }

  int32_t readLong() {
    return read<int64_t>();
  }

  float readFloat() {
    return read<float>();
  }

  double readDouble() {
    return read<double>();
  }

  size_t remaining() const {
    return size_ - position_;
  }

  void skip(size_t size) {
    if (position_ + size > size_) {
      VELOX_USER_FAIL("SliceInput read exceeds buffer size.");
    }
    position_ += size;
  }

 private:
  template <typename T>
  T read() {
    if (position_ + sizeof(T) > size_) {
      VELOX_USER_FAIL("SliceInput read exceeds buffer size.");
    }
    T value;
    std::memcpy(&value, &data_[position_], sizeof(T));
    position_ += sizeof(T);
    return value;
  }

  const uint8_t* data_;
  size_t size_;
  size_t position_;
};

template <typename StringWriter>
class SliceOutput {
 public:
  SliceOutput(StringWriter& stringWriter) : stringWriter_(stringWriter) {}

  SliceOutput() = delete;

  void writeByte(uint8_t value) {
    write<uint8_t>(value);
  }

  void writeShort(int16_t value) {
    write<int16_t>(value);
  }

  void writeInt(int32_t value) {
    write<int32_t>(value);
  }

  void writeLong(int64_t value) {
    write<int64_t>(value);
  }

  void writeFloat(float value) {
    write<float>(value);
  }

  void writeDouble(double value) {
    write<double>(value);
  }

  void writeBytes(const char* data, size_t size) {
    stringWriter_.append(std::string_view(data, size));
  }

 private:
  template <typename T>
  void write(const T& value) {
    stringWriter_.append(
        std::string_view(reinterpret_cast<const char*>(&value), sizeof(T)));
  }

  StringWriter& stringWriter_;
};

/// Deserialize Velox's internal format to a geometry.  Do not call this within
/// GEOS_TRY macro: it will catch the exceptions that need to bubble up.
std::unique_ptr<geos::geom::Geometry> deserializeGeometry(
    const StringView& geometryString);

/// Serialize geometry into Velox's internal format.  Do not call this within
/// GEOS_TRY macro: it will catch the exceptions that need to bubble up.
std::string serializeGeometry(const geos::geom::Geometry& geosGeometry);

} // namespace facebook::velox::functions::geospatial
