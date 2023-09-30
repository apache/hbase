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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import org.apache.hadoop.hbase.wal.WALProvider.WriterBase;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * Base class for combined wal writer implementations.
 */
@InterfaceAudience.Private
public class CombinedWriterBase<T extends WriterBase> implements WriterBase {

  private static final Logger LOG = LoggerFactory.getLogger(CombinedWriterBase.class);

  // the order of this list is not critical now as we have already solved the case where writing to
  // local succeed but remote fail, so implementation should implement concurrent sync to increase
  // performance
  protected final ImmutableList<T> writers;

  protected CombinedWriterBase(ImmutableList<T> writers) {
    this.writers = writers;
  }

  @Override
  public void close() throws IOException {
    Exception error = null;
    for (T writer : writers) {
      try {
        writer.close();
      } catch (Exception e) {
        LOG.warn("close writer failed", e);
        if (error == null) {
          error = e;
        }
      }
    }
    if (error != null) {
      throw new IOException("Failed to close at least one writer, please see the warn log above. "
        + "The cause is the first exception occurred", error);
    }
  }

  @Override
  public long getLength() {
    return writers.get(0).getLength();
  }

  @Override
  public long getSyncedLength() {
    return writers.get(0).getSyncedLength();
  }
}
