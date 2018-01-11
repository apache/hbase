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
import java.io.InterruptedIOException;
import java.util.concurrent.ExecutionException;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALProvider;
import org.apache.hadoop.hbase.wal.WALProvider.AsyncWriter;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

class WriterOverAsyncWriter implements WALProvider.Writer {

  private final WALProvider.AsyncWriter asyncWriter;

  public WriterOverAsyncWriter(AsyncWriter asyncWriter) {
    this.asyncWriter = asyncWriter;
  }

  @Override
  public void close() throws IOException {
    asyncWriter.close();
  }

  @Override
  public long getLength() {
    return asyncWriter.getLength();
  }

  @Override
  public void append(Entry entry) throws IOException {
    asyncWriter.append(entry);
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    try {
      asyncWriter.sync().get();
    } catch (InterruptedException e) {
      throw new InterruptedIOException();
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e.getCause(), IOException.class);
      throw new IOException(e.getCause());
    }
  }
}
