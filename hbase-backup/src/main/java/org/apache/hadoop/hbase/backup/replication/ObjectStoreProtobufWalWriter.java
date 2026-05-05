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
package org.apache.hadoop.hbase.backup.replication;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.asyncfs.monitor.StreamSlowMonitor;
import org.apache.hadoop.hbase.regionserver.wal.ProtobufLogWriter;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A custom implementation of {@link ProtobufLogWriter} that provides support for writing
 * protobuf-based WAL (Write-Ahead Log) entries to object store-backed files.
 * <p>
 * This class overrides the {@link ProtobufLogWriter#sync(boolean)} and
 * {@link ProtobufLogWriter#initOutput(FileSystem, Path, boolean, int, short, long, StreamSlowMonitor, boolean)}
 * methods to ensure compatibility with object stores, while ignoring specific capability checks
 * such as HFLUSH and HSYNC. These checks are often not supported by some object stores, and
 * bypassing them ensures smooth operation in such environments.
 * </p>
 */
@InterfaceAudience.Private
public class ObjectStoreProtobufWalWriter extends ProtobufLogWriter {
  private final AtomicLong syncedLength = new AtomicLong(0);

  @Override
  public void sync(boolean forceSync) throws IOException {
    FSDataOutputStream fsDataOutputstream = this.output;
    if (fsDataOutputstream == null) {
      return; // Presume closed
    }
    // Special case for Hadoop S3: Unlike traditional file systems, where flush() ensures data is
    // durably written, in Hadoop S3, flush() only writes data to the internal buffer and does not
    // immediately persist it to S3. The actual upload to S3 happens asynchronously, typically when
    // a block is full or when close() is called, which finalizes the upload process.
    fsDataOutputstream.flush();
    AtomicUtils.updateMax(this.syncedLength, fsDataOutputstream.getPos());
  }

  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
    short replication, long blockSize, StreamSlowMonitor monitor, boolean noLocalWrite)
    throws IOException {
    try {
      super.initOutput(fs, path, overwritable, bufferSize, replication, blockSize, monitor,
        noLocalWrite);
    } catch (CommonFSUtils.StreamLacksCapabilityException e) {
      // Ignore capability check for HFLUSH and HSYNC capabilities
      // Some object stores may not support these capabilities, so we bypass the exception handling
      // to ensure compatibility with such stores.
    }
  }
}
