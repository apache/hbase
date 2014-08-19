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
package org.apache.hadoop.hbase.jni;

import java.io.IOException;
import java.util.ArrayList;

import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

public class ScannerProxy extends RowProxy {

  private ClientProxy clientProxy_ = null;
  private Scanner scanner_ = null;
  private byte[] endRow_ = null;
  private int maxNumRows_ = -1;
  private int numVersions_ = -1;

  ScannerProxy(ClientProxy clientProxy) {
    this.clientProxy_ = clientProxy;
  }

  private void initScanner() {
    scanner_ = clientProxy_.newScanner(getTable());
    if (row_ != null) {
      scanner_.setStartKey(row_);
    }
    if (endRow_ != null) {
      scanner_.setStopKey(endRow_);
    }
    if (maxNumRows_ != -1) {
      scanner_.setMaxNumRows(maxNumRows_);
    }
    if (numVersions_ != -1) {
      scanner_.setMaxVersions(numVersions_);
    }
  }

  public void next(final long callback, final long scanner,
      final long extra) throws IOException {
    if (scanner_ == null) {
      initScanner();
    }
    scanner_.nextRows().addBoth(
        new ScannerNextCallback<Object, ArrayList<ArrayList<KeyValue>>>(
            this, callback, scanner, extra));
  }

  public void close(final long callback, final long scanner,
      final long extra) throws IOException {
    scanner_.close().addBoth(
        new ScannerCloseCallback<Object, Object>(
            callback, scanner, extra));
  }

  public void setMaxNumRows(int maxNumRows) {
    maxNumRows_ = maxNumRows;
  }

  public void setNumVersions(int numVersions) {
    numVersions_ = numVersions;
  }

  public void setEndRow(byte[] endRow) {
    endRow_ = endRow;
  }
}
