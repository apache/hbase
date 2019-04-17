/**
 *
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

package org.apache.hadoop.hbase.mob;

import java.io.Closeable;
import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The MobCell will maintain a {@link Cell} and a {@link StoreFileScanner} inside. Now, the mob cell
 * is backend by NIO ByteBuffers which are allocated from ByteBuffAllocator, so we cannot just read
 * the cell and close the MOB file scanner because the MOB file scanner closing will deallocate the
 * NIO ByteBuffers, which resulting memory leak.
 * <p>
 * Actually, the right solution is: <br>
 * 1. Read the normal cell; <br>
 * 2. Parse the value of normal cell and get MOB fileName,offset,length; <br>
 * 3. Open scanner to read the mob value; <br>
 * 4. Construct the response cell whose key is from the normal cell and value is from the mob cell.
 * <br>
 * 5. Ship the response cell to HBase client. <br>
 * 6. Release both normal cell's block and mob cell's block. <br>
 * <p>
 * For mob cell, the block releasing just means closing the the mob scanner, so here we need to keep
 * the {@link StoreFileScanner} inside and close only when we're ensure that the MobCell has been
 * shipped to RPC client.
 */
@InterfaceAudience.Private
public class MobCell implements Closeable {

  private final Cell cell;
  private final StoreFileScanner sfScanner;

  public MobCell(Cell cell) {
    this.cell = cell;
    this.sfScanner = null;
  }

  public MobCell(Cell cell, StoreFileScanner sfScanner) {
    this.cell = cell;
    this.sfScanner = sfScanner;
  }

  public Cell getCell() {
    return cell;
  }

  @Override
  public void close() throws IOException {
    if (this.sfScanner != null) {
      this.sfScanner.close();
    }
  }
}
