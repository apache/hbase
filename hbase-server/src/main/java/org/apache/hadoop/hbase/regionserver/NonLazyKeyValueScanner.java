/*
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
package org.apache.hadoop.hbase.regionserver;

import java.io.IOException;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;

/**
 * A "non-lazy" scanner which always does a real seek operation. Most scanners
 * are inherited from this class.
 */
@InterfaceAudience.Private
public abstract class NonLazyKeyValueScanner implements KeyValueScanner {

  @Override
  public boolean requestSeek(Cell kv, boolean forward, boolean useBloom)
      throws IOException {
    return doRealSeek(this, kv, forward);
  }

  @Override
  public boolean realSeekDone() {
    return true;
  }

  @Override
  public void enforceSeek() throws IOException {
    throw new NotImplementedException("enforceSeek must not be called on a " +
        "non-lazy scanner");
  }

  public static boolean doRealSeek(KeyValueScanner scanner,
      Cell kv, boolean forward) throws IOException {
    return forward ? scanner.reseek(kv) : scanner.seek(kv);
  }

  @Override
  public boolean shouldUseScanner(Scan scan, HStore store, long oldestUnexpiredTS) {
    // No optimizations implemented by default.
    return true;
  }

  @Override
  public boolean isFileScanner() {
    // Not a file by default.
    return false;
  }


  @Override
  public Path getFilePath() {
    // Not a file by default.
    return null;
  }

  @Override
  public Cell getNextIndexedKey() {
    return null;
  }

  @Override
  public void shipped() throws IOException {
    // do nothing
  }
}
