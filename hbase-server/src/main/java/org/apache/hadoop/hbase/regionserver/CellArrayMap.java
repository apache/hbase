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
 */
package org.apache.hadoop.hbase.regionserver;

import java.util.Comparator;
import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * CellArrayMap is a simple array of Cells and cannot be allocated off-heap. As all java arrays
 * CellArrayMap's array of references pointing to Cell objects.
 */
@InterfaceAudience.Private
public class CellArrayMap<T extends Cell> extends CellFlatMap<T> {

  private final T[] block;

  /*
   * The Cells Array is created only when CellArrayMap is created, all sub-CellBlocks use boundary
   * indexes. The given Cell array must be ordered.
   */
  public CellArrayMap(Comparator<? super T> comparator, T[] b, int min, int max,
    boolean descending) {
    super(comparator, min, max, descending);
    this.block = b;
  }

  /* To be used by base class only to create a sub-CellFlatMap */
  @Override
  protected CellFlatMap<T> createSubCellFlatMap(int min, int max, boolean descending) {
    return new CellArrayMap<>(comparator(), this.block, min, max, descending);
  }

  @Override
  protected T getCell(int i) {
    if ((i < minCellIdx) || (i >= maxCellIdx)) return null;
    return block[i];
  }
}
