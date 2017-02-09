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

#include <folly/io/IOBuf.h>
#include <memory>

#include "serde/cell-outputstream.h"
#include "serde/cell-scanner.h"

namespace hbase {

/**
 * @brief Encoder / Decoder for Cells.
 */
class Codec {
 public:
  virtual ~Codec() {}

  class Encoder : public CellOutputStream {};

  class Decoder : public CellScanner {};

  virtual std::unique_ptr<Encoder> CreateEncoder() = 0;
  virtual std::unique_ptr<Decoder> CreateDecoder(std::unique_ptr<folly::IOBuf> cell_block,
                                                 uint32_t cell_block_start_offset,
                                                 uint32_t cell_block_length) = 0;

  /** @brief returns the java class name corresponding to this Codec implementation */
  virtual const char* java_class_name() const = 0;
};

} /* namespace hbase */
