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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.io.Writable;

/**
 * Defines the procedure to atomically perform multiple scans and mutations
 * on one single row. The generic type parameter T is the return type of
 * RowProcessor.getResult().
 */
@InterfaceAudience.Public
public interface RowProcessor<T> extends Writable {

  /**
   * Which row to perform the read-write
   */
  byte[] getRow();

  /**
   * Obtain the processing result
   */
  T getResult();

  /**
   * Is this operation read only? If this is true, process() should not add
   * any mutations or it throws IOException.
   * @return ture if read only operation
   */
  boolean readOnly();

  /**
   * HRegion calls this to process a row. You should override this to create
   * your own RowProcessor.
   *
   * @param now the current system millisecond
   * @param scanner the call back object the can be used to scan the row
   * @param mutations the mutations for HRegion to do
   * @param walEdit the wal edit here allows inject some other meta data
   */
  void process(long now,
               RowProcessor.RowScanner scanner,
               List<KeyValue> mutations,
               WALEdit walEdit) throws IOException;

  /**
   * The call back provided by HRegion to perform the scans on the row
   */
  public interface RowScanner {
    /**
     * @param scan The object defines what to read
     * @param result The scan results will be added here
     */
    void doScan(Scan scan, List<KeyValue> result) throws IOException;
  }

  /**
   * @return The replication cluster id.
   */
  UUID getClusterId();

}
