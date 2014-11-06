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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * TODO javadoc
 */
@InterfaceAudience.Private
public abstract class BaseDecoder implements Codec.Decoder {
  protected static final Log LOG = LogFactory.getLog(BaseDecoder.class);
  protected final InputStream in;
  private boolean hasNext = true;
  private Cell current = null;

  public BaseDecoder(final InputStream in) {
    this.in = in;
  }

  @Override
  public boolean advance() throws IOException {
    if (!this.hasNext) return this.hasNext;
    if (this.in.available() == 0) {
      this.hasNext = false;
      return this.hasNext;
    }
    try {
      this.current = parseCell();
    } catch (IOException ioEx) {
      rethrowEofException(ioEx);
    }
    return this.hasNext;
  }

  private void rethrowEofException(IOException ioEx) throws IOException {
    boolean isEof = false;
    try {
      isEof = this.in.available() == 0;
    } catch (Throwable t) {
      LOG.trace("Error getting available for error message - ignoring", t);
    }
    if (!isEof) throw ioEx;
    if (LOG.isTraceEnabled()) {
      LOG.trace("Partial cell read caused by EOF", ioEx);
    }
    EOFException eofEx = new EOFException("Partial cell read");
    eofEx.initCause(ioEx);
    throw eofEx;
  }

  /**
   * @return extract a Cell
   * @throws IOException
   */
  protected abstract Cell parseCell() throws IOException;

  @Override
  public Cell current() {
    return this.current;
  }
}
