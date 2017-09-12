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

#pragma once

#include <folly/io/Cursor.h>
#include <folly/io/IOBuf.h>
#include <memory>

#include "hbase/client/cell.h"
#include "hbase/serde/codec.h"

namespace hbase {

/**
 * @brief Class for parsing sequence of Cells based on org.apache.hadoop.hbase.KeyValueCodec.java
 *
 * KeyValueCodec implements CellScanner interface. Sequence of cells are obtained from cell_block.
 * We have CreateEncoder and CreateDecoder public methods which will return Encoder/Decoder
 * instances which will be used to obtain individual cells in cell_block.
 * Usage:-
 * 1) Cell Decoding:-
 * unique_ptr<CellScanner> cell_scanner = KeyValueCodec::CreateDecoder(cell_block, cb_start_offset,
 *    cb_length);
 * while (cell_scanner->Advance()) {
 *  auto current_cell = cell_scanner->Current
 * }
 */
class KeyValueCodec : public Codec {
 public:
  /**
        * Constructor
        */
  KeyValueCodec() {}

  std::unique_ptr<Codec::Encoder> CreateEncoder() override { return std::make_unique<KVEncoder>(); }
  std::unique_ptr<Codec::Decoder> CreateDecoder(std::unique_ptr<folly::IOBuf> cell_block,
                                                uint32_t offset, uint32_t length) override {
    return std::make_unique<KVDecoder>(std::move(cell_block), offset, length);
  }

  /** @brief returns the java class name corresponding to this Codec implementation */
  virtual const char* java_class_name() const override { return kJavaClassName; }

  static constexpr const char* kJavaClassName = "org.apache.hadoop.hbase.codec.KeyValueCodec";

 private:
  class KVEncoder : public Codec::Encoder {
   public:
    KVEncoder() {}

    void Write(const Cell& cell) {
      // TODO: Encode Cells using KeyValueCodec wire format
    }

    void Flush() {}
  };

  class KVDecoder : public Codec::Decoder {
   public:
    KVDecoder(std::unique_ptr<folly::IOBuf> cell_block, uint32_t cell_block_start_offset,
              uint32_t cell_block_length);
    ~KVDecoder();

    /**
     * @brief Overridden from CellScanner. This method parses cell_block and stores the current in
     * current_cell_. Current cell can be obtained using cell_scanner.Current();
     */
    bool Advance();

    /**
     * @brief returns the current cell
     */
    const std::shared_ptr<Cell> Current() const { return current_cell_; }

    /**
     * @brief returns the total length of cell_meta_block
     */
    uint32_t CellBlockLength() const;

   private:
    std::shared_ptr<Cell> Decode(folly::io::Cursor& cursor);

    /**
     * Size of boolean in bytes
     */
    const int kHBaseSizeOfBoolean_ = sizeof(uint8_t) / sizeof(uint8_t);

    /**
     * Size of byte in bytes
     */
    const uint8_t kHBaseSizeOfByte_ = kHBaseSizeOfBoolean_;

    /**
     * Size of int in bytes
     */
    const uint32_t kHBaseSizeOfInt_ = sizeof(uint32_t) / kHBaseSizeOfByte_;

    /**
     * Size of long in bytes
     */
    const uint64_t kHBaseSizeOfLong_ = sizeof(uint64_t) / kHBaseSizeOfByte_;

    /**
     * Size of Short in bytes
     */
    const uint16_t kHBaseSizeOfShort_ = sizeof(uint16_t) / kHBaseSizeOfByte_;

    const uint32_t kHBaseSizeOfKeyLength_ = kHBaseSizeOfInt_;
    const uint32_t kHBaseSizeOfValueLength_ = kHBaseSizeOfInt_;
    const uint16_t kHBaseSizeOfRowLength_ = kHBaseSizeOfShort_;
    const uint8_t kHBaseSizeOfFamilyLength_ = kHBaseSizeOfByte_;
    const uint64_t kHBaseSizeOfTimestamp_ = kHBaseSizeOfLong_;
    const uint8_t kHBaseSizeOfKeyType_ = kHBaseSizeOfByte_;
    const uint32_t kHBaseSizeOfTimestampAndKey_ = kHBaseSizeOfTimestamp_ + kHBaseSizeOfKeyType_;
    const uint32_t kHBaseSizeOfKeyInfrastructure_ =
        kHBaseSizeOfRowLength_ + kHBaseSizeOfFamilyLength_ + kHBaseSizeOfTimestampAndKey_;
    const uint32_t kHBaseSizeOfKeyValueInfrastructure_ =
        kHBaseSizeOfKeyLength_ + kHBaseSizeOfValueLength_;

    std::unique_ptr<folly::IOBuf> cell_block_ = nullptr;
    uint32_t offset_ = 0;
    uint32_t length_ = 0;
    uint32_t cur_pos_ = 0;
    bool end_of_cell_block_ = false;

    std::shared_ptr<Cell> current_cell_ = nullptr;
  };
};

} /* namespace hbase */
