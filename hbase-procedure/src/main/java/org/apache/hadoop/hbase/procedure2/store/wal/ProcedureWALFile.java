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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.procedure2.store.ProcedureStoreTracker;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALHeader;
import org.apache.hadoop.hbase.protobuf.generated.ProcedureProtos.ProcedureWALTrailer;

/**
 * Describes a WAL File
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class ProcedureWALFile implements Comparable<ProcedureWALFile> {
  private static final Log LOG = LogFactory.getLog(ProcedureWALFile.class);

  private ProcedureWALHeader header;
  private FSDataInputStream stream;
  private FileStatus logStatus;
  private FileSystem fs;
  private Path logFile;
  private long startPos;
  private long minProcId;
  private long maxProcId;

  public ProcedureWALFile(final FileSystem fs, final FileStatus logStatus) {
    this.fs = fs;
    this.logStatus = logStatus;
    this.logFile = logStatus.getPath();
  }

  public ProcedureWALFile(FileSystem fs, Path logFile, ProcedureWALHeader header, long startPos) {
    this.fs = fs;
    this.logFile = logFile;
    this.header = header;
    this.startPos = startPos;
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
      return ProcedureWALFormat.readTrailer(stream, startPos, logStatus.getLen());
    } finally {
      stream.seek(startPos);
    }
  }

  public void readTracker(ProcedureStoreTracker tracker) throws IOException {
    ProcedureWALTrailer trailer = readTrailer();
    try {
      stream.seek(trailer.getTrackerPos());
      tracker.readFrom(stream);
    } finally {
      stream.seek(startPos);
    }
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

  public boolean isCompacted() {
    return header.getType() == ProcedureWALFormat.LOG_TYPE_COMPACTED;
  }

  public long getLogId() {
    return header.getLogId();
  }

  public long getSize() {
    return logStatus != null ? logStatus.getLen() : 0;
  }

  public void removeFile() throws IOException {
    close();
    fs.delete(logFile, false);
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