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
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALKey;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * A Protobuf based WAL has the following structure:
 * <p>
 * &lt;PB_WAL_MAGIC&gt;&lt;WALHeader&gt;&lt;WALEdits&gt;...&lt;WALEdits&gt;&lt;Trailer&gt;
 * &lt;TrailerSize&gt; &lt;PB_WAL_COMPLETE_MAGIC&gt;
 * </p>
 * The Reader reads meta information (WAL Compression state, WALTrailer, etc) in
 * {@link ProtobufLogReader#initReader(FSDataInputStream)}. A WALTrailer is an extensible structure
 * which is appended at the end of the WAL. This is empty for now; it can contain some meta
 * information such as Region level stats, etc in future.
 */
@InterfaceAudience.Private
public class ProtobufLogReader extends ReaderBase {
  private static final Log LOG = LogFactory.getLog(ProtobufLogReader.class);
  static final byte[] PB_WAL_MAGIC = Bytes.toBytes("PWAL");
  static final byte[] PB_WAL_COMPLETE_MAGIC = Bytes.toBytes("LAWP");
  private FSDataInputStream inputStream;
  private Codec.Decoder cellDecoder;
  private WALCellCodec.ByteStringUncompressor byteStringUncompressor;
  private boolean hasCompression = false;
  // walEditsStopOffset is the position of the last byte to read. After reading the last WALEdit entry
  // in the hlog, the inputstream's position is equal to walEditsStopOffset.
  private long walEditsStopOffset;
  private boolean trailerPresent;

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
    this.walEditsStopOffset = this.fileLength;
    long currentPosition = stream.getPos();
    trailerPresent = setTrailerIfPresent();
    this.seekOnFs(currentPosition);
    if (LOG.isTraceEnabled()) {
      LOG.trace("After reading the trailer: walEditsStopOffset: " + this.walEditsStopOffset
          + ", fileLength: " + this.fileLength + ", " + "trailerPresent: " + trailerPresent);
    }
  }

  /**
   * To check whether a trailer is present in a WAL, it seeks to position (fileLength -
   * PB_WAL_COMPLETE_MAGIC.size() - Bytes.SIZEOF_INT). It reads the int value to know the size of
   * the trailer, and checks whether the trailer is present at the end or not by comparing the last
   * PB_WAL_COMPLETE_MAGIC.size() bytes. In case trailer is not present, it returns false;
   * otherwise, sets the trailer and sets this.walEditsStopOffset variable up to the point just
   * before the trailer.
   * <ul>
   * The trailer is ignored in case:
   * <li>fileLength is 0 or not correct (when file is under recovery, etc).
   * <li>the trailer size is negative.
   * </ul>
   * <p>
   * In case the trailer size > this.trailerMaxSize, it is read after a WARN message.
   * @return true if a valid trailer is present
   * @throws IOException
   */
  private boolean setTrailerIfPresent() {
    try {
      long trailerSizeOffset = this.fileLength - (PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT);
      if (trailerSizeOffset <= 0) return false;// no trailer possible.
      this.seekOnFs(trailerSizeOffset);
      // read the int as trailer size.
      int trailerSize = this.inputStream.readInt();
      ByteBuffer buf = ByteBuffer.allocate(ProtobufLogReader.PB_WAL_COMPLETE_MAGIC.length);
      this.inputStream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
      if (!Arrays.equals(buf.array(), PB_WAL_COMPLETE_MAGIC)) {
        LOG.warn("No trailer found.");
        return false;
      }
      if (trailerSize < 0) {
        LOG.warn("Invalid trailer Size " + trailerSize + ", ignoring the trailer");
        return false;
      } else if (trailerSize > this.trailerWarnSize) {
        // continue reading after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum configured size : "
          + trailerSize + " > " + this.trailerWarnSize);
      }
      // seek to the position where trailer starts.
      long positionOfTrailer = trailerSizeOffset - trailerSize;
      this.seekOnFs(positionOfTrailer);
      // read the trailer.
      buf = ByteBuffer.allocate(trailerSize);// for trailer.
      this.inputStream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
      trailer = WALTrailer.parseFrom(buf.array());
      this.walEditsStopOffset = positionOfTrailer;
      return true;
    } catch (IOException ioe) {
      LOG.warn("Got IOE while reading the trailer. Continuing as if no trailer is present.", ioe);
    }
    return false;
  }

  @Override
  protected void initAfterCompression() throws IOException {
    WALCellCodec codec = WALCellCodec.create(this.conf, this.compressionContext);
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
      if (trailerPresent && this.inputStream.getPos() == this.walEditsStopOffset) return false;
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
      if (trailerPresent && this.inputStream.getPos() > this.walEditsStopOffset) {
        LOG.error("Read WALTrailer while reading WALEdits. hlog: " + this.path
            + ", inputStream.getPos(): " + this.inputStream.getPos() + ", walEditsStopOffset: "
            + this.walEditsStopOffset);
        throw new IOException("Read WALTrailer while reading WALEdits");
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
  public WALTrailer getWALTrailer() {
    return trailer;
  }

  @Override
  protected void seekOnFs(long pos) throws IOException {
    this.inputStream.seek(pos);
  }
}
