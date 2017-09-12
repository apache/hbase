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

namespace hbase {

class Cell;

/**
 * @brief Interface for iterating over a sequence of Cells
 */
class CellScanner {
 public:
  virtual ~CellScanner() {}

  /**
   * @brief This method will be used to iterate the cells.
   * Typical usage will be :-
   * while(cell_scanner.Advance()){
   *  auto current_cell = cell_scanner.Current();
   * }
   */
  virtual bool Advance() = 0;

  /**
   * @brief returns the current cell
   */
  virtual const std::shared_ptr<Cell> Current() const = 0;
};

} /* namespace hbase */
