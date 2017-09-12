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

#include <memory>

namespace hbase {

class Cell;

/**
 * @brief Encoder / Decoder for Cells.
 */
class CellOutputStream {
 public:
  virtual ~CellOutputStream() {}

  /**
   * Implementation must copy the entire state of the Cell. If the written Cell is modified
   * immediately after the write method returns, the modifications must have absolutely no effect
   * on the copy of the Cell that was added in the write.
   * @param cell Cell to write out
   * @throws IOException
   */
  virtual void Write(const Cell& cell) = 0;

  /**
   * Let the implementation decide what to do.  Usually means writing accumulated data into a
   * byte[] that can then be read from the implementation to be sent to disk, put in the block
   * cache, or sent over the network.
   * @throws IOException
   */
  virtual void Flush() = 0;
};

} /* namespace hbase */
