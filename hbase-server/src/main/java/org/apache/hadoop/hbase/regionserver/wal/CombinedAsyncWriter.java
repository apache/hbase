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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;

/**
 * An {@link AsyncWriter} wrapper which writes data to a set of {@link AsyncWriter} instances.
 */
@InterfaceAudience.Private
public final class CombinedAsyncWriter implements AsyncWriter {

  private static final Logger LOG = LoggerFactory.getLogger(CombinedAsyncWriter.class);

  private final ImmutableList<AsyncWriter> writers;

  private CombinedAsyncWriter(ImmutableList<AsyncWriter> writers) {
    this.writers = writers;
  }

  @Override
  public long getLength() {
    return writers.get(0).getLength();
  }

  @Override
  public void close() throws IOException {
    Exception error = null;
    for (AsyncWriter writer : writers) {
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
      throw new IOException("Failed to close at least one writer, please see the warn log above. " +
        "The cause is the first exception occured", error);
    }
  }

  @Override
  public void append(Entry entry) {
    writers.forEach(w -> w.append(entry));
  }

  @Override
  public CompletableFuture<Long> sync() {
    CompletableFuture<Long> future = new CompletableFuture<>();
    AtomicInteger remaining = new AtomicInteger(writers.size());
    writers.forEach(w -> w.sync().whenComplete((length, error) -> {
      if (error != null) {
        future.completeExceptionally(error);
        return;
      }
      if (remaining.decrementAndGet() == 0) {
        future.complete(length);
      }
    }));
    return future;
  }

  public static CombinedAsyncWriter create(AsyncWriter writer, AsyncWriter... writers) {
    return new CombinedAsyncWriter(
      ImmutableList.<AsyncWriter> builder().add(writer).add(writers).build());
  }
}
