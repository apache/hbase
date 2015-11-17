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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;

import com.google.common.annotations.VisibleForTesting;

/**
 * Buffers writes for Deletes in addition to Puts. Buffering Deletes can significantly speed up
 * MapReduce jobs. The order of both Mutation types is preserved in the write buffer, and a buffer
 * flush can be triggered by either Put or Delete operations.
 */
@InterfaceAudience.Private
public class BufferedHTable extends HTable {

  private boolean closed = false;

  public BufferedHTable(Configuration conf, String tableName) throws IOException {
    super(conf, tableName);
  }

  public BufferedHTable(Configuration conf, byte[] tableName) throws IOException {
    super(conf, tableName);
  }

  @Override
  public void delete(Delete delete) throws IOException {
    doDelete(delete);
  }

  @Override
  public void delete(List<Delete> deletes) throws IOException {
    for (Delete delete : deletes) {
      doDelete(delete);
    }
  }

  private void doDelete(Delete delete) throws IOException {
    if (this.closed) {
      throw new IllegalStateException("BufferedHTable was closed");
    }

    this.currentWriteBufferSize += delete.heapSize();
    this.writeAsyncBuffer.add(delete);

    // flush when write buffer size exceeds configured limit
    if (this.currentWriteBufferSize > getWriteBufferSize()) {
      flushCommits();
    }
  }

  @Override
  public void close() throws IOException {
    if (this.closed) {
      return;
    }
    flushCommits();
    this.closed = true;
    super.close();
  }

  @VisibleForTesting
  long getCurrentWriteBufferSize() {
    return this.currentWriteBufferSize;
  }
}
