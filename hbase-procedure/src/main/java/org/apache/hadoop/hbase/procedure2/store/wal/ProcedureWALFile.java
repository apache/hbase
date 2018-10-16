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
package org.apache.hadoop.hbase.procedure2.store.wal;

import java.io.IOException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.ProcedureProtos.ProcedureWALTrailer;

/**
 * Describes a WAL File
 */
@InterfaceAudience.Private
public class ProcedureWALFile implements Comparable<ProcedureWALFile> {
  private static final Logger LOG = LoggerFactory.getLogger(ProcedureWALFile.class);

  private ProcedureWALHeader header;
  private FSDataInputStream stream;
  private FileSystem fs;
  private Path logFile;
  private long startPos;
  private long minProcId;
  private long maxProcId;
  private long logSize;
  private long timestamp;

  public ProcedureStoreTracker getTracker() {
    return tracker;
  }

  private final ProcedureStoreTracker tracker = new ProcedureStoreTracker();

  public ProcedureWALFile(final FileSystem fs, final FileStatus logStatus) {
    this.fs = fs;
    this.logFile = logStatus.getPath();
    this.logSize = logStatus.getLen();
    this.timestamp = logStatus.getModificationTime();
    tracker.setPartialFlag(true);
  }

  public ProcedureWALFile(FileSystem fs, Path logFile, ProcedureWALHeader header,
      long startPos, long timestamp) {
    this.fs = fs;
    this.header = header;
    this.logFile = logFile;
    this.startPos = startPos;
    this.logSize = startPos;
    this.timestamp = timestamp;
    tracker.setPartialFlag(true);
  }

  public void open() throws IOException {
    if (stream == null) {
      stream = fs.open(logFile);
    }

    if (header == null) {
      header = ProcedureWALFormat.readHeader(stream);
      startPos = stream.getPos();
    } else {
      stream.seek(startPos);
    }
  }

  public ProcedureWALTrailer readTrailer() throws IOException {
    try {
      return ProcedureWALFormat.readTrailer(stream, startPos, logSize);
    } finally {
      stream.seek(startPos);
    }
  }

  public void readTracker() throws IOException {
    ProcedureWALTrailer trailer = readTrailer();
    try {
      stream.seek(trailer.getTrackerPos());
      final ProcedureProtos.ProcedureStoreTracker trackerProtoBuf =
          ProcedureProtos.ProcedureStoreTracker.parseDelimitedFrom(stream);
      tracker.resetToProto(trackerProtoBuf);
    } finally {
      stream.seek(startPos);
    }
  }

  public void updateLocalTracker(ProcedureStoreTracker tracker) {
    this.tracker.resetTo(tracker);
  }

  public void close() {
    if (stream == null) return;
    try {
      stream.close();
    } catch (IOException e) {
      LOG.warn("unable to close the wal file: " + logFile, e);
    } finally {
      stream = null;
    }
  }

  public FSDataInputStream getStream() {
    return stream;
  }

  public ProcedureWALHeader getHeader() {
    return header;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public boolean isCompacted() {
    return header.getType() == ProcedureWALFormat.LOG_TYPE_COMPACTED;
  }

  public long getLogId() {
    return header.getLogId();
  }

  public long getSize() {
    return logSize;
  }

  /**
   * Used to update in-progress log sizes. the FileStatus will report 0 otherwise.
   */
  void addToSize(long size) {
    this.logSize += size;
  }

  public void removeFile(final Path walArchiveDir) throws IOException {
    close();
    boolean archived = false;
    if (walArchiveDir != null) {
      Path archivedFile = new Path(walArchiveDir, logFile.getName());
      LOG.info("Archiving " + logFile + " to " + archivedFile);
      if (!fs.rename(logFile, archivedFile)) {
        LOG.warn("Failed archive of " + logFile + ", deleting");
      } else {
        archived = true;
      }
    }
    if (!archived) {
      if (!fs.delete(logFile, false)) {
        LOG.warn("Failed delete of " + logFile);
      }
    }
  }

  public void setProcIds(long minId, long maxId) {
    this.minProcId = minId;
    this.maxProcId = maxId;
  }

  public long getMinProcId() {
    return minProcId;
  }

  public long getMaxProcId() {
    return maxProcId;
  }

  @Override
  public int compareTo(final ProcedureWALFile other) {
    long diff = header.getLogId() - other.header.getLogId();
    return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ProcedureWALFile)) return false;
    return compareTo((ProcedureWALFile)o) == 0;
  }

  @Override
  public int hashCode() {
    return logFile.hashCode();
  }

  @Override
  public String toString() {
    return logFile.toString();
  }
}
