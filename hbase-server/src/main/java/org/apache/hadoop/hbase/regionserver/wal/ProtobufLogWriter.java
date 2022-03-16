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
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.StreamCapabilities;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.AtomicUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.CommonFSUtils.StreamLacksCapabilityException;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;

/**
 * Writer for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class ProtobufLogWriter extends AbstractProtobufLogWriter
    implements FSHLogProvider.Writer {

  private static final Logger LOG = LoggerFactory.getLogger(ProtobufLogWriter.class);

  protected FSDataOutputStream output;

  private final AtomicLong syncedLength = new AtomicLong(0);

  @Override
  public void append(Entry entry) throws IOException {
    entry.getKey().getBuilder(compressor).
        setFollowingKvCount(entry.getEdit().size()).build().writeDelimitedTo(output);
    for (Cell cell : entry.getEdit().getCells()) {
      // cellEncoder must assume little about the stream, since we write PB and cells in turn.
      cellEncoder.write(cell);
    }
    length.set(output.getPos());
  }

  @Override
  public void close() throws IOException {
    if (this.output != null) {
      try {
        if (!trailerWritten) {
          writeWALTrailer();
        }
        this.output.close();
      } catch (NullPointerException npe) {
        // Can get a NPE coming up from down in DFSClient$DFSOutputStream#close
        LOG.warn(npe.toString(), npe);
      }
      this.output = null;
    }
  }

  @Override
  public void sync(boolean forceSync) throws IOException {
    FSDataOutputStream fsdos = this.output;
    if (fsdos == null) {
      return; // Presume closed
    }
    fsdos.flush();
    if (forceSync) {
      fsdos.hsync();
    } else {
      fsdos.hflush();
    }
    AtomicUtils.updateMax(this.syncedLength, fsdos.getPos());
  }

  @Override
  public long getSyncedLength() {
    return this.syncedLength.get();
  }

  public FSDataOutputStream getStream() {
    return this.output;
  }

  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize) throws IOException, StreamLacksCapabilityException {
    this.output = CommonFSUtils.createForWal(fs, path, overwritable, bufferSize, replication,
        blockSize, false);
    if (fs.getConf().getBoolean(CommonFSUtils.UNSAFE_STREAM_CAPABILITY_ENFORCE, true)) {
      if (!output.hasCapability(StreamCapabilities.HFLUSH)) {
        throw new StreamLacksCapabilityException(StreamCapabilities.HFLUSH);
      }
      if (!output.hasCapability(StreamCapabilities.HSYNC)) {
        throw new StreamLacksCapabilityException(StreamCapabilities.HSYNC);
      }
    }
  }

  @Override
  protected void closeOutput() {
    if (this.output != null) {
      try {
        this.output.close();
      } catch (IOException e) {
        LOG.warn("Close output failed", e);
      }
    }
  }

  @Override
  protected long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException {
    output.write(magic);
    header.writeDelimitedTo(output);
    return output.getPos();
  }

  @Override
  protected OutputStream getOutputStreamForCellEncoder() {
    return this.output;
  }

  @Override
  protected long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic) throws IOException {
    trailer.writeTo(output);
    output.writeInt(trailer.getSerializedSize());
    output.write(magic);
    return output.getPos();
  }
}
