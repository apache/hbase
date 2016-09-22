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

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;
import org.apache.hadoop.hbase.regionserver.Store;

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
  public Cell peek() {
    return delegate.peek();
  }

  @Override
  public Cell next() throws IOException {
    return delegate.next();
  }

  @Override
  public boolean seek(Cell key) throws IOException {
    return delegate.seek(key);
  }

  @Override
  public boolean reseek(Cell key) throws IOException {
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
  public boolean shouldUseScanner(Scan scan, Store store, long oldestUnexpiredTS) {
    return delegate.shouldUseScanner(scan, store, oldestUnexpiredTS);
  }

  @Override
  public boolean requestSeek(Cell kv, boolean forward, boolean useBloom) throws IOException {
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
  public boolean backwardSeek(Cell key) throws IOException {
    return delegate.backwardSeek(key);
  }

  @Override
  public boolean seekToPreviousRow(Cell key) throws IOException {
    return delegate.seekToPreviousRow(key);
  }

  @Override
  public boolean seekToLastRow() throws IOException {
    return delegate.seekToLastRow();
  }

  @Override
  public Cell getNextIndexedKey() {
    return delegate.getNextIndexedKey();
  }
}