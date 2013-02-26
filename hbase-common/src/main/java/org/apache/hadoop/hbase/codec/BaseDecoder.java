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
package org.apache.hadoop.hbase.codec;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.hbase.Cell;

abstract class BaseDecoder implements Codec.Decoder {
  final InputStream in;
  private boolean hasNext = true;
  private Cell current = null;

  BaseDecoder(final InputStream in) {
    this.in = in;
  }

  @Override
  public boolean advance() {
    if (!this.hasNext) return this.hasNext;
    try {
      if (this.in.available() <= 0) {
        this.hasNext = false;
        return this.hasNext;
      }
      this.current = parseCell();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return this.hasNext;
  }

  /**
   * @return extract a Cell
   * @throws IOException
   */
  abstract Cell parseCell() throws IOException;

  @Override
  public Cell current() {
    return this.current;
  }
}
