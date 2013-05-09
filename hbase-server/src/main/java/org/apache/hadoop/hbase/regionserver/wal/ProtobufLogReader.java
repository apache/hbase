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

import java.io.EOFException;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALKey;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Reader for protobuf-based WAL.
 */
@InterfaceAudience.Private
public class ProtobufLogReader extends ReaderBase {
  private static final Log LOG = LogFactory.getLog(ProtobufLogReader.class);
  static final byte[] PB_WAL_MAGIC = Bytes.toBytes("PWAL");

  private FSDataInputStream inputStream;
  private Codec.Decoder cellDecoder;
  private WALCellCodec.ByteStringUncompressor byteStringUncompressor;
  private boolean hasCompression = false;

  public ProtobufLogReader() {
    super();
  }

  @Override
  public void close() throws IOException {
    if (this.inputStream != null) {
      this.inputStream.close();
      this.inputStream = null;
    }
  }

  @Override
  public long getPosition() throws IOException {
    return inputStream.getPos();
  }

  @Override
  public void reset() throws IOException {
    initInternal(null, false);
    initAfterCompression(); // We need a new decoder (at least).
  }

  @Override
  protected void initReader(FSDataInputStream stream) throws IOException {
    initInternal(stream, true);
  }

  private void initInternal(FSDataInputStream stream, boolean isFirst) throws IOException {
    close();
    long expectedPos = PB_WAL_MAGIC.length;
    if (stream == null) {
      stream = fs.open(path);
      stream.seek(expectedPos);
    }
    if (stream.getPos() != expectedPos) {
      throw new IOException("The stream is at invalid position: " + stream.getPos());
    }
    // Initialize metadata or, when we reset, just skip the header.
    WALProtos.WALHeader.Builder builder = WALProtos.WALHeader.newBuilder();
    boolean hasHeader = builder.mergeDelimitedFrom(stream);
    if (!hasHeader) {
      throw new EOFException("Couldn't read WAL PB header");
    }
    if (isFirst) {
      WALProtos.WALHeader header = builder.build();
      this.hasCompression = header.hasHasCompression() && header.getHasCompression();
    }
    this.inputStream = stream;

  }

  @Override
  protected void initAfterCompression() throws IOException {
    WALCellCodec codec = new WALCellCodec(this.compressionContext);
    this.cellDecoder = codec.getDecoder(this.inputStream);
    if (this.hasCompression) {
      this.byteStringUncompressor = codec.getByteStringUncompressor();
    }
  }

  @Override
  protected boolean hasCompression() {
    return this.hasCompression;
  }

  @Override
  protected boolean readNext(HLog.Entry entry) throws IOException {
    while (true) {
      WALKey.Builder builder = WALKey.newBuilder();
      boolean hasNext = false;
      try {
        hasNext = builder.mergeDelimitedFrom(inputStream);
      } catch (InvalidProtocolBufferException ipbe) {
        LOG.error("Invalid PB while reading WAL, probably an unexpected EOF, ignoring", ipbe);
      }
      if (!hasNext) return false;
      if (!builder.isInitialized()) {
        // TODO: not clear if we should try to recover from corrupt PB that looks semi-legit.
        //       If we can get the KV count, we could, theoretically, try to get next record.
        LOG.error("Partial PB while reading WAL, probably an unexpected EOF, ignoring");
        return false;
      }
      WALKey walKey = builder.build();
      entry.getKey().readFieldsFromPb(walKey, this.byteStringUncompressor);
      if (!walKey.hasFollowingKvCount() || 0 == walKey.getFollowingKvCount()) {
        LOG.warn("WALKey has no KVs that follow it; trying the next one");
        continue;
      }
      int expectedCells = walKey.getFollowingKvCount();
      long posBefore = this.inputStream.getPos();
      try {
        int actualCells = entry.getEdit().readFromCells(cellDecoder, expectedCells);
        if (expectedCells != actualCells) {
          throw new EOFException("Only read " + actualCells); // other info added in catch
        }
      } catch (Exception ex) {
        String posAfterStr = "<unknown>";
        try {
          posAfterStr = this.inputStream.getPos() + "";
        } catch (Throwable t) {
           LOG.trace("Error getting pos for error message - ignoring", t);
        }
        String message = " while reading " + expectedCells + " WAL KVs; started reading at "
            + posBefore + " and read up to " + posAfterStr;
        IOException realEofEx = extractHiddenEof(ex);
        if (realEofEx != null) {
          LOG.error("EOF " + message, realEofEx);
          return false;
        }
        message = "Error " + message;
        LOG.error(message);
        throw new IOException(message, ex);
      }
      return true;
    }
  }

  private IOException extractHiddenEof(Exception ex) {
    // There are two problems we are dealing with here. Hadoop stream throws generic exception
    // for EOF, not EOFException; and scanner further hides it inside RuntimeException.
    IOException ioEx = null;
    if (ex instanceof EOFException) {
      return (EOFException)ex;
    } else if (ex instanceof IOException) {
      ioEx = (IOException)ex;
    } else if (ex instanceof RuntimeException
        && ex.getCause() != null && ex.getCause() instanceof IOException) {
      ioEx = (IOException)ex.getCause();
    }
    if (ioEx != null) {
      if (ioEx.getMessage().contains("EOF")) return ioEx;
      return null;
    }
    return null;
  }

  @Override
  protected void seekOnFs(long pos) throws IOException {
    this.inputStream.seek(pos);
  }
}
