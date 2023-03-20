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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.Closeable;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.security.Key;
import java.security.KeyException;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Decryptor;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.wal.AbstractFSWALProvider;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ImmutableList;
import org.apache.hbase.thirdparty.com.google.common.io.Closeables;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos;
import org.apache.hadoop.hbase.shaded.protobuf.generated.WALProtos.WALTrailer;

/**
 * Base class for reading protobuf based wal reader
 */
@InterfaceAudience.Private
public abstract class AbstractProtobufWALReader
  implements AbstractFSWALProvider.Initializer, Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractProtobufWALReader.class);

  // public for WALFactory until we move everything to o.a.h.h.wal
  public static final byte[] PB_WAL_MAGIC = Bytes.toBytes("PWAL");

  // public for TestWALSplit
  public static final byte[] PB_WAL_COMPLETE_MAGIC = Bytes.toBytes("LAWP");

  /**
   * Configuration name of WAL Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, providers should log a warning. e.g. this is used with Protobuf reader/writer.
   */
  static final String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  static final int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB

  private static final List<String> WRITER_CLS_NAMES = ImmutableList.of(
    ProtobufLogWriter.class.getSimpleName(), AsyncProtobufLogWriter.class.getSimpleName(),
    "SecureProtobufLogWriter", "SecureAsyncProtobufLogWriter");

  protected Configuration conf;

  protected FileSystem fs;

  protected Path path;

  protected long fileLength;

  protected FSDataInputStream inputStream;

  protected CompressionContext compressionCtx;
  protected boolean hasCompression = false;
  protected boolean hasTagCompression = false;
  protected boolean hasValueCompression = false;
  protected Compression.Algorithm valueCompressionType;

  protected Codec.Decoder cellDecoder;
  protected WALCellCodec.ByteStringUncompressor byteStringUncompressor;

  protected long walEditsStopOffset;
  protected boolean trailerPresent;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  // cell codec classname
  protected String codecClsName;

  protected Decryptor decryptor;

  /**
   * Get or create the input stream used by cell decoder.
   * <p/>
   * For implementing replication, we may need to limit the bytes we can read, so here we provide a
   * method so sub classes can wrap the original input stream.
   */
  protected abstract InputStream getCellCodecInputStream(FSDataInputStream stream);

  /**
   * Skip to the given position.
   */
  protected abstract void skipTo(long position) throws IOException;

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, long startPosition)
    throws IOException {
    this.conf = conf;
    this.path = path;
    this.fs = fs;
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);

    Pair<FSDataInputStream, FileStatus> pair = open();
    FSDataInputStream stream = pair.getFirst();
    FileStatus stat = pair.getSecond();
    boolean initSucceeded = false;
    try {
      // read the header
      WALProtos.WALHeader header = readHeader(stream);
      // initialize metadata and fields
      initDecryptor(header);
      initCompression(header);
      initWALCellCodec(header, getCellCodecInputStream(stream));

      // read trailer if available
      readTrailer(stream, stat);

      // this is intentional as we do not want the above methods to use the inputStream field. For
      // implementation tailing reader, we need to wrap the input stream when creating cell decoder,
      // so we need to make sure in the above methods we do not accidentally use the stored
      // inputStream directly and cause trouble. If a method needs to use an input stream, we just
      // pass the input stream in, like readHeader and readTrailer.
      this.inputStream = stream;

      // seek to the given position if it is not -1
      if (startPosition >= 0 && startPosition != inputStream.getPos()) {
        if (compressionCtx != null) {
          // skip to the position, as we need to construct the compression dictionary
          skipTo(startPosition);
        } else {
          // just seek to the position
          stream.seek(startPosition);
        }
      }
      initSucceeded = true;
    } finally {
      if (!initSucceeded) {
        Closeables.close(stream, initSucceeded);
        inputStream = null;
      }
    }
  }

  private Pair<FSDataInputStream, FileStatus> openArchivedWAL() throws IOException {
    Path archivedWAL = AbstractFSWALProvider.findArchivedLog(path, conf);
    if (archivedWAL != null) {
      // try open from oldWAL dir
      return Pair.newPair(fs.open(archivedWAL), fs.getFileStatus(archivedWAL));
    } else {
      return null;
    }
  }

  protected final Pair<FSDataInputStream, FileStatus> open() throws IOException {
    try {
      return Pair.newPair(fs.open(path), fs.getFileStatus(path));
    } catch (FileNotFoundException e) {
      Pair<FSDataInputStream, FileStatus> pair = openArchivedWAL();
      if (pair != null) {
        return pair;
      } else {
        throw e;
      }
    } catch (RemoteException re) {
      IOException ioe = re.unwrapRemoteException(FileNotFoundException.class);
      if (!(ioe instanceof FileNotFoundException)) {
        throw ioe;
      }
      Pair<FSDataInputStream, FileStatus> pair = openArchivedWAL();
      if (pair != null) {
        return pair;
      } else {
        throw ioe;
      }
    }
  }

  protected final WALProtos.WALHeader readHeader(FSDataInputStream stream) throws IOException {
    byte[] magic = new byte[PB_WAL_MAGIC.length];
    try {
      stream.readFully(magic);
    } catch (EOFException e) {
      throw new WALHeaderEOFException("EOF while reading PB WAL magic", e);
    }
    if (!Arrays.equals(PB_WAL_MAGIC, magic)) {
      throw new IOException("Invalid PB WAL magic " + Bytes.toStringBinary(magic) + ", expected "
        + Bytes.toStringBinary(PB_WAL_MAGIC));
    }
    WALProtos.WALHeader header;
    try {
      header = ProtobufUtil.parseDelimitedFrom(stream, WALProtos.WALHeader.parser());
    } catch (InvalidProtocolBufferException e) {
      if (ProtobufUtil.isEOF(e)) {
        throw new WALHeaderEOFException("EOF while reading PB header", e);
      } else {
        throw e;
      }
    } catch (EOFException e) {
      throw new WALHeaderEOFException("EOF while reading PB header", e);
    }
    if (header == null) {
      throw new WALHeaderEOFException("EOF while reading PB header");
    }
    if (header.hasWriterClsName() && !getWriterClsNames().contains(header.getWriterClsName())) {
      throw new IOException("Got unknown writer class: " + header.getWriterClsName());
    }
    return header;
  }

  private void initDecryptor(WALProtos.WALHeader header) throws IOException {
    if (!header.hasEncryptionKey()) {
      return;
    }
    EncryptionTest.testKeyProvider(conf);
    EncryptionTest.testCipherProvider(conf);

    // Retrieve a usable key
    byte[] keyBytes = header.getEncryptionKey().toByteArray();
    Key key = null;
    String walKeyName = conf.get(HConstants.CRYPTO_WAL_KEY_NAME_CONF_KEY);
    // First try the WAL key, if one is configured
    if (walKeyName != null) {
      try {
        key = EncryptionUtil.unwrapWALKey(conf, walKeyName, keyBytes);
      } catch (KeyException e) {
        LOG.debug("Unable to unwrap key with WAL key '{}'", walKeyName, e);
        key = null;
      }
    }
    if (key == null) {
      String masterKeyName =
        conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName());
      try {
        // Then, try the cluster master key
        key = EncryptionUtil.unwrapWALKey(conf, masterKeyName, keyBytes);
      } catch (KeyException e) {
        // If the current master key fails to unwrap, try the alternate, if
        // one is configured
        LOG.debug("Unable to unwrap key with current master key '{}'", masterKeyName, e);
        String alternateKeyName = conf.get(HConstants.CRYPTO_MASTERKEY_ALTERNATE_NAME_CONF_KEY);
        if (alternateKeyName != null) {
          try {
            key = EncryptionUtil.unwrapWALKey(conf, alternateKeyName, keyBytes);
          } catch (KeyException ex) {
            throw new IOException(ex);
          }
        } else {
          throw new IOException(e);
        }
      }
    }

    // Use the algorithm the key wants
    Cipher cipher = Encryption.getCipher(conf, key.getAlgorithm());
    if (cipher == null) {
      throw new IOException("Cipher '" + key.getAlgorithm() + "' is not available");
    }

    // Set up the decryptor for this WAL

    decryptor = cipher.getDecryptor();
    decryptor.setKey(key);

    LOG.debug("Initialized secure protobuf WAL: cipher={}", cipher.getName());
  }

  private void initCompression(WALProtos.WALHeader header) throws IOException {
    this.hasCompression = header.hasHasCompression() && header.getHasCompression();
    if (!hasCompression) {
      return;
    }
    this.hasTagCompression = header.hasHasTagCompression() && header.getHasTagCompression();
    this.hasValueCompression = header.hasHasValueCompression() && header.getHasValueCompression();
    if (header.hasValueCompressionAlgorithm()) {
      try {
        this.valueCompressionType =
          Compression.Algorithm.values()[header.getValueCompressionAlgorithm()];
      } catch (ArrayIndexOutOfBoundsException e) {
        throw new IOException("Invalid compression type", e);
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug(
        "Initializing compression context for {}: isRecoveredEdits={}"
          + ", hasTagCompression={}, hasValueCompression={}, valueCompressionType={}",
        path, CommonFSUtils.isRecoveredEdits(path), hasTagCompression, hasValueCompression,
        valueCompressionType);
    }
    try {
      compressionCtx =
        new CompressionContext(LRUDictionary.class, CommonFSUtils.isRecoveredEdits(path),
          hasTagCompression, hasValueCompression, valueCompressionType);
    } catch (Exception e) {
      throw new IOException("Failed to initialize CompressionContext", e);
    }
  }

  private WALCellCodec getCodec(Configuration conf, String cellCodecClsName,
    CompressionContext compressionContext) throws IOException {
    return WALCellCodec.create(conf, cellCodecClsName, compressionContext);
  }

  protected final void initWALCellCodec(WALProtos.WALHeader header, InputStream inputStream)
    throws IOException {
    String cellCodecClsName = header.hasCellCodecClsName() ? header.getCellCodecClsName() : null;
    if (decryptor != null && SecureWALCellCodec.class.getName().equals(cellCodecClsName)) {
      WALCellCodec codec = SecureWALCellCodec.getCodec(this.conf, decryptor);
      this.cellDecoder = codec.getDecoder(inputStream);
      // We do not support compression with WAL encryption
      this.compressionCtx = null;
      this.byteStringUncompressor = WALCellCodec.getNoneUncompressor();
      this.hasCompression = false;
      this.hasTagCompression = false;
      this.hasValueCompression = false;
    } else {
      WALCellCodec codec = getCodec(conf, cellCodecClsName, compressionCtx);
      this.cellDecoder = codec.getDecoder(inputStream);
      if (this.hasCompression) {
        this.byteStringUncompressor = codec.getByteStringUncompressor();
      } else {
        this.byteStringUncompressor = WALCellCodec.getNoneUncompressor();
      }
    }
    this.codecClsName = cellCodecClsName;
  }

  protected final void readTrailer(FSDataInputStream stream, FileStatus stat) throws IOException {
    this.fileLength = stat.getLen();
    this.walEditsStopOffset = this.fileLength;
    long currentPos = stream.getPos();
    // we will reset walEditsStopOffset if trailer if available
    trailerPresent = setTrailerIfPresent(stream);
    if (currentPos != stream.getPos()) {
      // seek back
      stream.seek(currentPos);
    }
  }

  /**
   * To check whether a trailer is present in a WAL, it seeks to position (fileLength -
   * PB_WAL_COMPLETE_MAGIC.size() - Bytes.SIZEOF_INT). It reads the int value to know the size of
   * the trailer, and checks whether the trailer is present at the end or not by comparing the last
   * PB_WAL_COMPLETE_MAGIC.size() bytes. In case trailer is not present, it returns false;
   * otherwise, sets the trailer and sets this.walEditsStopOffset variable up to the point just
   * before the trailer.
   * <p/>
   * The trailer is ignored in case:
   * <ul>
   * <li>fileLength is 0 or not correct (when file is under recovery, etc).
   * <li>the trailer size is negative.
   * </ul>
   * In case the trailer size > this.trailerMaxSize, it is read after a WARN message.
   * @return true if a valid trailer is present
   */
  private boolean setTrailerIfPresent(FSDataInputStream stream) throws IOException {
    try {
      long trailerSizeOffset = this.fileLength - (PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT);
      if (trailerSizeOffset <= 0) {
        // no trailer possible.
        return false;
      }
      stream.seek(trailerSizeOffset);
      // read the int as trailer size.
      int trailerSize = stream.readInt();
      ByteBuffer buf = ByteBuffer.allocate(PB_WAL_COMPLETE_MAGIC.length);
      stream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
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
      stream.seek(positionOfTrailer);
      // read the trailer.
      buf = ByteBuffer.allocate(trailerSize);// for trailer.
      stream.readFully(buf.array(), buf.arrayOffset(), buf.capacity());
      trailer = WALTrailer.parseFrom(buf.array());
      this.walEditsStopOffset = positionOfTrailer;
      return true;
    } catch (IOException ioe) {
      LOG.warn("Got IOE while reading the trailer. Continuing as if no trailer is present.", ioe);
    }
    return false;
  }

  protected final boolean reachWALEditsStopOffset(long pos) {
    if (trailerPresent && pos > 0 && pos == walEditsStopOffset) {
      LOG.trace("Reached end of expected edits area at offset {}", pos);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Returns names of the accepted writer classes
   */
  public List<String> getWriterClsNames() {
    return WRITER_CLS_NAMES;
  }

  /**
   * Returns the cell codec classname
   */
  public String getCodecClsName() {
    return codecClsName;
  }

  public long getPosition() throws IOException {
    return inputStream != null ? inputStream.getPos() : -1;
  }

  public long trailerSize() {
    if (trailerPresent) {
      // sizeof PB_WAL_COMPLETE_MAGIC + sizeof trailerSize + trailer
      final long calculatedSize =
        (long) PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT + trailer.getSerializedSize();
      final long expectedSize = fileLength - walEditsStopOffset;
      if (expectedSize != calculatedSize) {
        LOG.warn("After parsing the trailer, we expect the total footer to be {} bytes, but we "
          + "calculate it as being {}", expectedSize, calculatedSize);
      }
      return expectedSize;
    } else {
      return -1L;
    }
  }

  protected final String getPositionQuietly() {
    try {
      long pos = getPosition();
      return pos >= 0 ? Long.toString(pos) : "<unknown>";
    } catch (Exception e) {
      LOG.warn("failed to get position, ignoring", e);
      return "<unknown>";
    }
  }

  protected final IOException extractHiddenEof(Exception ex) {
    // There are two problems we are dealing with here. Hadoop stream throws generic exception
    // for EOF, not EOFException; and scanner further hides it inside RuntimeException.
    IOException ioEx = null;
    if (ex instanceof EOFException) {
      return (EOFException) ex;
    } else if (ex instanceof IOException) {
      ioEx = (IOException) ex;
    } else if (
      ex instanceof RuntimeException && ex.getCause() != null
        && ex.getCause() instanceof IOException
    ) {
      ioEx = (IOException) ex.getCause();
    }
    if ((ioEx != null) && (ioEx.getMessage() != null)) {
      if (ioEx.getMessage().contains("EOF")) {
        return ioEx;
      }
      return null;
    }
    return null;
  }

  /**
   * This is used to determine whether we have already reached the WALTrailer. As the size and magic
   * are at the end of the WAL file, it is possible that these two options are missing while
   * writing, so we will consider there is no trailer. And when we actually reach the WALTrailer, we
   * will try to decode it as WALKey and we will fail but the error could be vary as it is parsing
   * WALTrailer actually.
   * @return whether this is a WALTrailer and we should throw EOF to upper layer the file is done
   */
  protected final boolean isWALTrailer(long startPosition) throws IOException {
    // We have nothing in the WALTrailer PB message now so its size is just a int length size and a
    // magic at the end
    int trailerSize = PB_WAL_COMPLETE_MAGIC.length + Bytes.SIZEOF_INT;
    if (fileLength - startPosition >= trailerSize) {
      // We still have more than trailerSize bytes before reaching the EOF so this is not a trailer.
      // We also test for == here because if this is a valid trailer, we can read it while opening
      // the reader so we should not reach here
      return false;
    }
    inputStream.seek(startPosition);
    for (int i = 0; i < 4; i++) {
      int r = inputStream.read();
      if (r == -1) {
        // we have reached EOF while reading the length, and all bytes read are 0, so we assume this
        // is a partial trailer
        return true;
      }
      if (r != 0) {
        // the length is not 0, should not be a trailer
        return false;
      }
    }
    for (int i = 0; i < PB_WAL_COMPLETE_MAGIC.length; i++) {
      int r = inputStream.read();
      if (r == -1) {
        // we have reached EOF while reading the magic, and all bytes read are matched, so we assume
        // this is a partial trailer
        return true;
      }
      if (r != (PB_WAL_COMPLETE_MAGIC[i] & 0xFF)) {
        // does not match magic, should not be a trailer
        return false;
      }
    }
    // in fact we should not reach here, as this means the trailer bytes are all matched and
    // complete, then we should not call this method...
    return true;
  }

  @Override
  public void close() {
    if (inputStream != null) {
      IOUtils.closeQuietly(inputStream);
      inputStream = null;
    }
  }
}
