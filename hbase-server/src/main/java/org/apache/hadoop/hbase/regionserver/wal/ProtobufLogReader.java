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
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.LimitInputStream;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader.Builder;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALKey;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.CodedInputStream;
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
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX, HBaseInterfaceAudience.CONFIG})
public class ProtobufLogReader extends ReaderBase {
  private static final Log LOG = LogFactory.getLog(ProtobufLogReader.class);
  static final byte[] PB_WAL_MAGIC = Bytes.toBytes("PWAL");
  static final byte[] PB_WAL_COMPLETE_MAGIC = Bytes.toBytes("LAWP");
  protected FSDataInputStream inputStream;
  protected Codec.Decoder cellDecoder;
  protected WALCellCodec.ByteStringUncompressor byteStringUncompressor;
  protected boolean hasCompression = false;
  protected boolean hasTagCompression = false;
  // walEditsStopOffset is the position of the last byte to read. After reading the last WALEdit entry
  // in the hlog, the inputstream's position is equal to walEditsStopOffset.
  private long walEditsStopOffset;
  private boolean trailerPresent;
  private static List<String> writerClsNames = new ArrayList<String>();
  static {
    writerClsNames.add(ProtobufLogWriter.class.getSimpleName());
  }

  enum WALHdrResult {
    EOF,                   // stream is at EOF when method starts
    SUCCESS,
    UNKNOWN_WRITER_CLS     // name of writer class isn't recognized
  }
  
  // context for WALHdr carrying information such as Cell Codec classname
  static class WALHdrContext {
    WALHdrResult result;
    String cellCodecClsName;
    
    WALHdrContext(WALHdrResult result, String cellCodecClsName) {
      this.result = result;
      this.cellCodecClsName = cellCodecClsName;
    }
    WALHdrResult getResult() {
      return result;
    }
    String getCellCodecClsName() {
      return cellCodecClsName;
    }
  }

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
    String clsName = initInternal(null, false);
    initAfterCompression(clsName); // We need a new decoder (at least).
  }

  @Override
  protected String initReader(FSDataInputStream stream) throws IOException {
    return initInternal(stream, true);
  }

  /*
   * Returns names of the accepted writer classes
   */
  protected List<String> getWriterClsNames() {
    return writerClsNames;
  }

  protected WALHdrContext readHeader(Builder builder, FSDataInputStream stream)
      throws IOException {
     boolean res = builder.mergeDelimitedFrom(stream);
     if (!res) return new WALHdrContext(WALHdrResult.EOF, null);
     if (builder.hasWriterClsName() &&
         !getWriterClsNames().contains(builder.getWriterClsName())) {
       return new WALHdrContext(WALHdrResult.UNKNOWN_WRITER_CLS, null);
     }
     String clsName = null;
     if (builder.hasCellCodecClsName()) {
       clsName = builder.getCellCodecClsName();
     }
     return new WALHdrContext(WALHdrResult.SUCCESS, clsName);
  }

  private String initInternal(FSDataInputStream stream, boolean isFirst)
      throws IOException {
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
    WALHdrContext hdrCtxt = readHeader(builder, stream);
    WALHdrResult walHdrRes = hdrCtxt.getResult();
    if (walHdrRes == WALHdrResult.EOF) {
      throw new EOFException("Couldn't read WAL PB header");
    }
    if (walHdrRes == WALHdrResult.UNKNOWN_WRITER_CLS) {
      throw new IOException("Got unknown writer class: " + builder.getWriterClsName());
    }
    if (isFirst) {
      WALProtos.WALHeader header = builder.build();
      this.hasCompression = header.hasHasCompression() && header.getHasCompression();
      this.hasTagCompression = header.hasHasTagCompression() && header.getHasTagCompression();
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
    return hdrCtxt.getCellCodecClsName();
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
        LOG.trace("No trailer found.");
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

  protected WALCellCodec getCodec(Configuration conf, String cellCodecClsName,
      CompressionContext compressionContext) throws IOException {
    return WALCellCodec.create(conf, cellCodecClsName, compressionContext);
  }

  @Override
  protected void initAfterCompression() throws IOException {
    initAfterCompression(null);
  }
  
  @Override
  protected void initAfterCompression(String cellCodecClsName) throws IOException {
    WALCellCodec codec = getCodec(this.conf, cellCodecClsName, this.compressionContext);
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
  protected boolean hasTagCompression() {
    return this.hasTagCompression;
  }

  @Override
  protected boolean readNext(HLog.Entry entry) throws IOException {
    while (true) {
      // OriginalPosition might be < 0 on local fs; if so, it is useless to us.
      long originalPosition = this.inputStream.getPos();
      if (trailerPresent && originalPosition > 0 && originalPosition == this.walEditsStopOffset) {
        return false;
      }
      WALKey.Builder builder = WALKey.newBuilder();
      long size = 0;
      try {
        long available = -1;
        try {
          int firstByte = this.inputStream.read();
          if (firstByte == -1) {
            throw new EOFException("First byte is negative");
          }
          size = CodedInputStream.readRawVarint32(firstByte, this.inputStream);
          // available may be < 0 on local fs for instance.  If so, can't depend on it.
          available = this.inputStream.available();
          if (available > 0 && available < size) {
            throw new EOFException("Available stream not enough for edit, " +
                "inputStream.available()= " + this.inputStream.available() + ", " +
                "entry size= " + size);
          }
          final InputStream limitedInput = new LimitInputStream(this.inputStream, size);
          builder.mergeFrom(limitedInput);
        } catch (InvalidProtocolBufferException ipbe) {
          throw (EOFException) new EOFException("Invalid PB, EOF? Ignoring; originalPosition=" +
            originalPosition + ", currentPosition=" + this.inputStream.getPos() +
            ", messageSize=" + size + ", currentAvailable=" + available).initCause(ipbe);
        }
        if (!builder.isInitialized()) {
          // TODO: not clear if we should try to recover from corrupt PB that looks semi-legit.
          //       If we can get the KV count, we could, theoretically, try to get next record.
          throw new EOFException("Partial PB while reading WAL, " +
              "probably an unexpected EOF, ignoring");
        }
        WALKey walKey = builder.build();
        entry.getKey().readFieldsFromPb(walKey, this.byteStringUncompressor);
        if (!walKey.hasFollowingKvCount() || 0 == walKey.getFollowingKvCount()) {
          LOG.trace("WALKey has no KVs that follow it; trying the next one");
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
          throw (EOFException) new EOFException("EOF " + message).
              initCause(realEofEx != null ? realEofEx : ex);
        }
        if (trailerPresent && this.inputStream.getPos() > this.walEditsStopOffset) {
          LOG.error("Read WALTrailer while reading WALEdits. hlog: " + this.path
              + ", inputStream.getPos(): " + this.inputStream.getPos() + ", walEditsStopOffset: "
              + this.walEditsStopOffset);
          throw new EOFException("Read WALTrailer while reading WALEdits");
        }
      } catch (EOFException eof) {
        LOG.trace("Encountered a malformed edit, seeking back to last good position in file", eof);
        // If originalPosition is < 0, it is rubbish and we cannot use it (probably local fs)
        if (originalPosition < 0) throw eof;
        // Else restore our position to original location in hope that next time through we will
        // read successfully.
        seekOnFs(originalPosition);
        return false;
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
