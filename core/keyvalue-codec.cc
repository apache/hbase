/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include "core/keyvalue-codec.h"

#include <string>

namespace hbase {

KeyValueCodec::KVDecoder::KVDecoder(std::unique_ptr<folly::IOBuf> cell_block, uint32_t offset,
                                    uint32_t length)
    : cell_block_(std::move(cell_block)), offset_(offset), length_(length) {}

KeyValueCodec::KVDecoder::~KVDecoder() {}

std::shared_ptr<Cell> KeyValueCodec::KVDecoder::Decode(folly::io::Cursor &cursor) {
  uint32_t key_length = cursor.readBE<uint32_t>();
  uint32_t value_length = cursor.readBE<uint32_t>();
  uint16_t row_length = cursor.readBE<uint16_t>();
  std::string row = cursor.readFixedString(row_length);
  uint8_t column_family_length = cursor.readBE<uint8_t>();
  std::string column_family = cursor.readFixedString(column_family_length);
  int qualifier_length =
      key_length - (row_length + column_family_length + kHBaseSizeOfKeyInfrastructure_);
  std::string column_qualifier = cursor.readFixedString(qualifier_length);
  uint64_t timestamp = cursor.readBE<uint64_t>();
  uint8_t key_type = cursor.readBE<uint8_t>();
  std::string value = cursor.readFixedString(value_length);

  return std::make_shared<Cell>(row, column_family, column_qualifier, timestamp, value,
                                static_cast<hbase::CellType>(key_type));
}

bool KeyValueCodec::KVDecoder::Advance() {
  if (end_of_cell_block_) {
    return false;
  }

  if (cur_pos_ == length_) {
    end_of_cell_block_ = true;
    return false;
  }

  folly::io::Cursor cursor(cell_block_.get());
  cursor.skip(offset_ + cur_pos_);
  uint32_t current_cell_size = cursor.readBE<uint32_t>();
  current_cell_ = Decode(cursor);
  cur_pos_ += kHBaseSizeOfInt_ + current_cell_size;
  return true;
}

uint32_t KeyValueCodec::KVDecoder::CellBlockLength() const { return length_; }
} /* namespace hbase */
