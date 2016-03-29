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

import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.DEFAULT_WAL_TRAILER_WARN_SIZE;
import static org.apache.hadoop.hbase.regionserver.wal.ProtobufLogReader.WAL_TRAILER_WARN_SIZE;

import java.io.IOException;
import java.io.OutputStream;
import java.security.Key;
import java.security.SecureRandom;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.codec.Codec;
import org.apache.hadoop.hbase.io.crypto.Cipher;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.Encryptor;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALHeader;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.EncryptionTest;
import org.apache.hadoop.hbase.util.FSUtils;

/**
 * Base class for Protobuf log writer.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public abstract class AbstractProtobufLogWriter {

  private static final Log LOG = LogFactory.getLog(AbstractProtobufLogWriter.class);

  protected CompressionContext compressionContext;
  protected Configuration conf;
  protected Codec.Encoder cellEncoder;
  protected WALCellCodec.ByteStringCompressor compressor;
  protected boolean trailerWritten;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;

  protected AtomicLong length = new AtomicLong();

  private WALCellCodec getCodec(Configuration conf, CompressionContext compressionContext)
      throws IOException {
    return WALCellCodec.create(conf, null, compressionContext);
  }

  private WALHeader buildWALHeader0(Configuration conf, WALHeader.Builder builder) {
    if (!builder.hasWriterClsName()) {
      builder.setWriterClsName(getWriterClassName());
    }
    if (!builder.hasCellCodecClsName()) {
      builder.setCellCodecClsName(WALCellCodec.getWALCellCodecClass(conf));
    }
    return builder.build();
  }

  protected WALHeader buildWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    return buildWALHeader0(conf, builder);
  }

  // should be called in sub classes's buildWALHeader method to build WALHeader for secure
  // environment. Do not forget to override the setEncryptor method as it will be called in this
  // method to init your encryptor.
  protected final WALHeader buildSecureWALHeader(Configuration conf, WALHeader.Builder builder)
      throws IOException {
    builder.setWriterClsName(getWriterClassName());
    if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false)) {
      EncryptionTest.testKeyProvider(conf);
      EncryptionTest.testCipherProvider(conf);

      // Get an instance of our cipher
      final String cipherName =
          conf.get(HConstants.CRYPTO_WAL_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
      Cipher cipher = Encryption.getCipher(conf, cipherName);
      if (cipher == null) {
        throw new RuntimeException("Cipher '" + cipherName + "' is not available");
      }

      // Generate an encryption key for this WAL
      SecureRandom rng = new SecureRandom();
      byte[] keyBytes = new byte[cipher.getKeyLength()];
      rng.nextBytes(keyBytes);
      Key key = new SecretKeySpec(keyBytes, cipher.getName());
      builder.setEncryptionKey(ByteStringer.wrap(EncryptionUtil.wrapKey(conf,
          conf.get(HConstants.CRYPTO_WAL_KEY_NAME_CONF_KEY,
              conf.get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY,
                  User.getCurrent().getShortName())),
          key)));

      // Set up the encryptor
      Encryptor encryptor = cipher.getEncryptor();
      encryptor.setKey(key);
      setEncryptor(encryptor);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Initialized secure protobuf WAL: cipher=" + cipher.getName());
      }
    }
    builder.setCellCodecClsName(SecureWALCellCodec.class.getName());
    return buildWALHeader0(conf, builder);
  }

  // override this if you need a encryptor
  protected void setEncryptor(Encryptor encryptor) {
  }

  protected String getWriterClassName() {
    return getClass().getSimpleName();
  }

  private boolean initializeCompressionContext(Configuration conf, Path path) throws IOException {
    boolean doCompress = conf.getBoolean(HConstants.ENABLE_WAL_COMPRESSION, false);
    if (doCompress) {
      try {
        this.compressionContext = new CompressionContext(LRUDictionary.class,
            FSUtils.isRecoveredEdits(path),
            conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true));
      } catch (Exception e) {
        throw new IOException("Failed to initiate CompressionContext", e);
      }
    }
    return doCompress;
  }

  public void init(FileSystem fs, Path path, Configuration conf, boolean overwritable)
      throws IOException {
    this.conf = conf;
    boolean doCompress = initializeCompressionContext(conf, path);
    this.trailerWarnSize = conf.getInt(WAL_TRAILER_WARN_SIZE, DEFAULT_WAL_TRAILER_WARN_SIZE);
    int bufferSize = FSUtils.getDefaultBufferSize(fs);
    short replication = (short) conf.getInt("hbase.regionserver.hlog.replication",
      FSUtils.getDefaultReplication(fs, path));
    long blockSize = conf.getLong("hbase.regionserver.hlog.blocksize",
      FSUtils.getDefaultBlockSize(fs, path));

    initOutput(fs, path, overwritable, bufferSize, replication, blockSize);

    boolean doTagCompress = doCompress
        && conf.getBoolean(CompressionContext.ENABLE_WAL_TAGS_COMPRESSION, true);
    length.set(writeMagicAndWALHeader(ProtobufLogReader.PB_WAL_MAGIC, buildWALHeader(conf,
      WALHeader.newBuilder().setHasCompression(doCompress).setHasTagCompression(doTagCompress))));

    initAfterHeader(doCompress);

    // instantiate trailer to default value.
    trailer = WALTrailer.newBuilder().build();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Initialized protobuf WAL=" + path + ", compression=" + doCompress);
    }
  }

  private void initAfterHeader0(boolean doCompress) throws IOException {
    WALCellCodec codec = getCodec(conf, this.compressionContext);
    this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
    if (doCompress) {
      this.compressor = codec.getByteStringCompressor();
    }
  }

  protected void initAfterHeader(boolean doCompress) throws IOException {
    initAfterHeader0(doCompress);
  }

  // should be called in sub classes's initAfterHeader method to init SecureWALCellCodec.
  protected final void secureInitAfterHeader(boolean doCompress, Encryptor encryptor)
      throws IOException {
    if (conf.getBoolean(HConstants.ENABLE_WAL_ENCRYPTION, false) && encryptor != null) {
      WALCellCodec codec = SecureWALCellCodec.getCodec(this.conf, encryptor);
      this.cellEncoder = codec.getEncoder(getOutputStreamForCellEncoder());
      // We do not support compression
      this.compressionContext = null;
    } else {
      initAfterHeader0(doCompress);
    }
  }

  void setWALTrailer(WALTrailer walTrailer) {
    this.trailer = walTrailer;
  }

  public long getLength() {
    return length.get();
  }

  private WALTrailer buildWALTrailer(WALTrailer.Builder builder) {
    return builder.build();
  }

  protected void writeWALTrailer() {
    try {
      int trailerSize = 0;
      if (this.trailer == null) {
        // use default trailer.
        LOG.warn("WALTrailer is null. Continuing with default.");
        this.trailer = buildWALTrailer(WALTrailer.newBuilder());
        trailerSize = this.trailer.getSerializedSize();
      } else if ((trailerSize = this.trailer.getSerializedSize()) > this.trailerWarnSize) {
        // continue writing after warning the user.
        LOG.warn("Please investigate WALTrailer usage. Trailer size > maximum size : " + trailerSize
            + " > " + this.trailerWarnSize);
      }
      length.set(writeWALTrailerAndMagic(trailer, ProtobufLogReader.PB_WAL_COMPLETE_MAGIC));
      this.trailerWritten = true;
    } catch (IOException ioe) {
      LOG.warn("Failed to write trailer, non-fatal, continuing...", ioe);
    }
  }

  protected abstract void initOutput(FileSystem fs, Path path, boolean overwritable, int bufferSize,
      short replication, long blockSize) throws IOException;

  /**
   * return the file length after written.
   */
  protected abstract long writeMagicAndWALHeader(byte[] magic, WALHeader header) throws IOException;

  protected abstract long writeWALTrailerAndMagic(WALTrailer trailer, byte[] magic)
      throws IOException;

  protected abstract OutputStream getOutputStreamForCellEncoder();
}
