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
import java.io.OutputStream;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * TODO javadoc
 */

@InterfaceAudience.Private
public abstract class BaseEncoder implements Codec.Encoder {
  protected final OutputStream out;
  // This encoder is 'done' once flush has been called.
  protected boolean flushed = false;

  public BaseEncoder(final OutputStream out) {
    this.out = out;
  }

  @Override
  public abstract void write(Cell cell) throws IOException;

  protected void checkFlushed() throws CodecException {
    if (this.flushed) throw new CodecException("Flushed; done");
  }

  @Override
  public void flush() throws IOException {
    if (this.flushed) return;
    this.flushed = true;
    this.out.flush();
  }
}
