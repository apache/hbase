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

import java.io.IOException;
import java.util.function.IntConsumer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.client.Scan;

public class DelegatingKeyValueScanner implements KeyValueScanner {
  protected KeyValueScanner delegate;

  public DelegatingKeyValueScanner(KeyValueScanner delegate) {
    this.delegate = delegate;
  }

  @Override
  public void shipped() throws IOException {
    delegate.shipped();
  }

  @Override
  public ExtendedCell peek() {
    return delegate.peek();
  }

  @Override
  public ExtendedCell next() throws IOException {
    return delegate.next();
  }

  @Override
  public boolean seek(ExtendedCell key) throws IOException {
    return delegate.seek(key);
  }

  @Override
  public boolean reseek(ExtendedCell key) throws IOException {
    return delegate.reseek(key);
  }

  @Override
  public long getScannerOrder() {
    return delegate.getScannerOrder();
  }

  @Override
  public void close() {
    delegate.close();
  }

  @Override
  public boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS) {
    return delegate.shouldUseScanner(scan, store, oldestUnexpiredTS);
  }

  @Override
  public boolean requestSeek(ExtendedCell kv, boolean forward, boolean useBloom)
    throws IOException {
    return delegate.requestSeek(kv, forward, useBloom);
  }

  @Override
  public boolean realSeekDone() {
    return delegate.realSeekDone();
  }

  @Override
  public void enforceSeek() throws IOException {
    delegate.enforceSeek();
  }

  @Override
  public boolean isFileScanner() {
    return delegate.isFileScanner();
  }

  @Override
  public Path getFilePath() {
    return delegate.getFilePath();
  }

  @Override
  public boolean backwardSeek(ExtendedCell key) throws IOException {
    return delegate.backwardSeek(key);
  }

  @Override
  public boolean seekToPreviousRow(ExtendedCell key) throws IOException {
    return delegate.seekToPreviousRow(key);
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    return delegate.seekToLastRow();
  }

  @Override
  public ExtendedCell getNextIndexedKey() {
    return delegate.getNextIndexedKey();
  }

  @Override
  public void recordBlockSize(IntConsumer blockSizeConsumer) {
    delegate.recordBlockSize(blockSizeConsumer);
  }
}
