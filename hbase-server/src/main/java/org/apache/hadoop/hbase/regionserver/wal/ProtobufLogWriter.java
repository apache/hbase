/**
 *
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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.wal.FSHLogProvider;
import org.apache.hadoop.hbase.wal.WAL.Entry;

/**
 * Writer for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class ProtobufLogWriter extends AbstractProtobufLogWriter
    implements FSHLogProvider.Writer {

  private static final Log LOG = LogFactory.getLog(ProtobufLogWriter.class);

  protected FSDataOutputStream output;

  @Override
  public void append(Entry entry) throws IOException {
    entry.setCompressionContext(compressionContext);
    entry.getKey().getBuilder(compressor).setFollowingKvCount(entry.getEdit().size()).build()
        .writeDelimitedTo(output);
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
        if (!trailerWritten) writeWALTrailer();
        this.output.close();
      } catch (NullPointerException npe) {
        // Can get a NPE coming up from down in DFSClient$DFSOutputStream#close
        LOG.warn(npe);
      }
      this.output = null;
    }
  }

  @Override
  public void sync() throws IOException {
    FSDataOutputStream fsdos = this.output;
    if (fsdos == null) return; // Presume closed
    fsdos.flush();
    fsdos.hflush();
  }

  public FSDataOutputStream getStream() {
    return this.output;
  }

  @SuppressWarnings("deprecation")
  @Override
  protected void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize) throws IOException {
    this.output = fs.createNonRecursive(path, overwritable, bufferSize, replication, blockSize,
      null);
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
