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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.hbase.util.FutureUtils;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALProvider.Writer;
import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;

@InterfaceAudience.Private
public final class CombinedWriter extends CombinedWriterBase<Writer> implements Writer {

  private final ImmutableList<ExecutorService> executors;

  private CombinedWriter(ImmutableList<Writer> writers) {
    super(writers);
    ImmutableList.Builder<ExecutorService> builder =
      ImmutableList.builderWithExpectedSize(writers.size() - 1);
    for (int i = 0; i < writers.size() - 1; i++) {
      Writer writer = writers.get(i);
      builder.add(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
        .setNameFormat("WAL-Writer-" + writer + "-%d").setDaemon(true).build()));
    }
    this.executors = builder.build();
  }

  private interface Action {
    void action(Writer writer) throws IOException;
  }

  private void apply(Action action) throws IOException {
    List<Future<?>> futures = new ArrayList<>(writers.size() - 1);
    for (int i = 0; i < writers.size() - 1; i++) {
      Writer writer = writers.get(i);
      futures.add(executors.get(i).submit(new Callable<Void>() {

        @Override
        public Void call() throws Exception {
          action.action(writer);
          return null;
        }
      }));
    }
    action.action(writers.get(writers.size() - 1));
    for (Future<?> future : futures) {
      FutureUtils.get(future);
    }
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    apply(writer -> writer.sync(forceSync));
  }

  @Override
  public void append(Entry entry) throws IOException {
    apply(writer -> writer.append(entry));
  }

  @Override
  public void close() throws IOException {
    executors.forEach(ExecutorService::shutdown);
    super.close();
  }

  public static CombinedWriter create(Writer writer, Writer... writers) {
    return new CombinedWriter(ImmutableList.<Writer> builder().add(writer).add(writers).build());
  }
}
