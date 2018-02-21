/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Cellersion 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY CellIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.regionserver;

import java.nio.ByteBuffer;

import org.apache.hadoop.hbase.Cell;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.ClassSize;

import java.util.Comparator;


/**
 * CellChunkMap is an array of serialized representations of Cell
 * (pointing to Chunks with full Cell data) and can be allocated both off-heap and on-heap.
 *
 * CellChunkMap is a byte array (chunk) holding all that is needed to access a Cell, which
 * is actually saved on another deeper chunk.
 * Per Cell we have a reference to this deeper byte array B (chunk ID, integer),
 * offset in bytes in B (integer), length in bytes in B (integer) and seqID of the cell (long).
 * In order to save reference to byte array we use the Chunk's ID given by ChunkCreator.
 *
 * The CellChunkMap memory layout on chunk A relevant to a deeper byte array B,
 * holding the actual cell data:
 *
 * < header > <---------------     first Cell     -----------------> <-- second Cell ...
 * --------------------------------------------------------------------------------------- ...
 *  integer  | integer      | integer      | integer     | long     |
 *  4 bytes  | 4 bytes      | 4 bytes      | 4 bytes     | 8 bytes  |
 *  ChunkID  | chunkID of   | offset in B  | length of   | sequence |          ...
 *  of this  | chunk B with | where Cell's | Cell's      | ID of    |
 *  chunk A  | Cell data    | data starts  | data in B   | the Cell |
 * --------------------------------------------------------------------------------------- ...
 */
@InterfaceAudience.Private
public class CellChunkMap extends CellFlatMap {

  private final Chunk[] chunks;             // the array of chunks, on which the index is based

  // number of cell-representations in a chunk
  // depends on the size of the chunks (may be index chunks or regular data chunks)
  // each chunk starts with its own ID following the cells data
  private final int numOfCellRepsInChunk;

  /**
   * C-tor for creating CellChunkMap from existing Chunk array, which must be ordered
   * (decreasingly or increasingly according to parameter "descending")
   * @param comparator a tool for comparing cells
   * @param chunks ordered array of index chunk with cell representations
   * @param min the index of the first cell (usually 0)
   * @param max number of Cells or the index of the cell after the maximal cell
   * @param descending the order of the given array
   */
  public CellChunkMap(Comparator<? super Cell> comparator,
      Chunk[] chunks, int min, int max, boolean descending) {
    super(comparator, min, max, descending);
    this.chunks = chunks;
    if (chunks != null && chunks.length != 0 && chunks[0] != null) {
      this.numOfCellRepsInChunk = (chunks[0].size - ChunkCreator.SIZEOF_CHUNK_HEADER) /
              ClassSize.CELL_CHUNK_MAP_ENTRY;
    } else { // In case the chunks array was not allocated
      this.numOfCellRepsInChunk = 0;
    }
  }

  /* To be used by base (CellFlatMap) class only to create a sub-CellFlatMap
  * Should be used only to create only CellChunkMap from CellChunkMap */
  @Override
  protected CellFlatMap createSubCellFlatMap(int min, int max, boolean descending) {
    return new CellChunkMap(this.comparator(), this.chunks, min, max, descending);
  }


  @Override
  protected Cell getCell(int i) {
    // get the index of the relevant chunk inside chunk array
    int chunkIndex = (i / numOfCellRepsInChunk);
    ByteBuffer block = chunks[chunkIndex].getData();// get the ByteBuffer of the relevant chunk
    int j = i - chunkIndex * numOfCellRepsInChunk; // get the index of the cell-representation

    // find inside the offset inside the chunk holding the index, skip bytes for chunk id
    int offsetInBytes = ChunkCreator.SIZEOF_CHUNK_HEADER + j* ClassSize.CELL_CHUNK_MAP_ENTRY;

    // find the chunk holding the data of the cell, the chunkID is stored first
    int chunkId = ByteBufferUtils.toInt(block, offsetInBytes);
    Chunk chunk = ChunkCreator.getInstance().getChunk(chunkId);
    if (chunk == null) {
      // this should not happen
      throw new IllegalArgumentException("In CellChunkMap, cell must be associated with chunk."
          + ". We were looking for a cell at index " + i);
    }

    // find the offset of the data of the cell, skip integer for chunkID, offset is stored second
    int offsetOfCell = ByteBufferUtils.toInt(block, offsetInBytes + Bytes.SIZEOF_INT);
    // find the length of the data of the cell, skip two integers for chunkID and offset,
    // length is stored third
    int lengthOfCell = ByteBufferUtils.toInt(block, offsetInBytes + 2*Bytes.SIZEOF_INT);
    // find the seqID of the cell, skip three integers for chunkID, offset, and length
    // the seqID is plain written as part of the cell representation
    long cellSeqID = ByteBufferUtils.toLong(block, offsetInBytes + 3*Bytes.SIZEOF_INT);

    ByteBuffer buf = chunk.getData();   // get the ByteBuffer where the cell data is stored
    if (buf == null) {
      // this should not happen
      throw new IllegalArgumentException("In CellChunkMap, chunk must be associated with ByteBuffer."
          + " Chunk: " + chunk + " Chunk ID: " + chunk.getId() + ", is from pool: "
          + chunk.isFromPool() + ". We were looking for a cell at index " + i);
    }

    return new ByteBufferChunkKeyValue(buf, offsetOfCell, lengthOfCell, cellSeqID);
  }
}
