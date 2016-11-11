/**
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

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * A memstore-local allocation buffer.
 * <p>
 * The MemStoreLAB is basically a bump-the-pointer allocator that allocates big (2MB) chunks from
 * and then doles it out to threads that request slices into the array. These chunks can get pooled
 * as well. See {@link MemStoreChunkPool}.
 * <p>
 * The purpose of this is to combat heap fragmentation in the regionserver. By ensuring that all
 * Cells in a given memstore refer only to large chunks of contiguous memory, we ensure that
 * large blocks get freed up when the memstore is flushed.
 * <p>
 * Without the MSLAB, the byte array allocated during insertion end up interleaved throughout the
 * heap, and the old generation gets progressively more fragmented until a stop-the-world compacting
 * collection occurs.
 * <p>
 * This manages the large sized chunks. When Cells are to be added to Memstore, MemStoreLAB's
 * {@link #copyCellInto(Cell)} gets called. This allocates enough size in the chunk to hold this
 * cell's data and copies into this area and then recreate a Cell over this copied data.
 * <p>
 * @see MemStoreChunkPool
 */
@InterfaceAudience.Private
public interface MemStoreLAB {

  String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  boolean USEMSLAB_DEFAULT = true;

  /**
   * Allocates slice in this LAB and copy the passed Cell into this area. Returns new Cell instance
   * over the copied the data. When this MemStoreLAB can not copy this Cell, it returns null.
   */
  Cell copyCellInto(Cell cell);

  /**
   * Close instance since it won't be used any more, try to put the chunks back to pool
   */
  void close();

  /**
   * Called when opening a scanner on the data of this MemStoreLAB
   */
  void incScannerCount();

  /**
   * Called when closing a scanner on the data of this MemStoreLAB
   */
  void decScannerCount();

}
