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
package org.apache.hadoop.hbase.wal;

import java.io.EOFException;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A helper class for writing UTs, where we will eat the EOF and return null when reaching EOF, so
 * in UTs we do not need to deal with partial WAL files if this does not affect the correctness. In
 * production code you usually you should not do this, as it may cause data loss if you always
 * ignore the EOFException.
 */
public final class NoEOFWALStreamReader implements WALStreamReader {

  private static final Logger LOG = LoggerFactory.getLogger(NoEOFWALStreamReader.class);

  private WALStreamReader reader;

  private NoEOFWALStreamReader(WALStreamReader reader) {
    this.reader = reader;
  }

  @Override
  public Entry next(Entry reuse) throws IOException {
    try {
      return reader.next(reuse);
    } catch (EOFException e) {
      LOG.warn("Got EOF while reading", e);
      return null;
    }
  }

  @Override
  public long getPosition() throws IOException {
    return reader.getPosition();
  }

  @Override
  public void close() {
    reader.close();
  }

  private int count() throws IOException {
    int count = 0;
    Entry entry = new Entry();
    while (next(entry) != null) {
      count++;
    }
    return count;
  }

  public static NoEOFWALStreamReader create(FileSystem fs, Path path, Configuration conf)
    throws IOException {
    return new NoEOFWALStreamReader(WALFactory.createStreamReader(fs, path, conf));
  }

  public static NoEOFWALStreamReader create(WALFactory walFactory, FileSystem fs, Path path)
    throws IOException {
    return new NoEOFWALStreamReader(walFactory.createStreamReader(fs, path));
  }

  public static int count(FileSystem fs, Path path, Configuration conf) throws IOException {
    try (NoEOFWALStreamReader reader = create(fs, path, conf)) {
      return reader.count();
    }
  }

  public static int count(WALFactory walFactory, FileSystem fs, Path path) throws IOException {
    try (NoEOFWALStreamReader reader = create(walFactory, fs, path)) {
      return reader.count();
    }
  }
}
