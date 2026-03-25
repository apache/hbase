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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.EnumMap;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.util.Dictionary;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Context that holds the various dictionaries for compression in WAL.
 * <p>
 * CompressionContexts are not expected to be shared among threads. Multithreaded use may produce
 * unexpected results.
 */
@InterfaceAudience.LimitedPrivate({ HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX })
public class CompressionContext {

  private static final Logger LOG = LoggerFactory.getLogger(CompressionContext.class);

  public static final String ENABLE_WAL_TAGS_COMPRESSION =
    "hbase.regionserver.wal.tags.enablecompression";

  public static final String ENABLE_WAL_VALUE_COMPRESSION =
    "hbase.regionserver.wal.value.enablecompression";

  public static final String WAL_VALUE_COMPRESSION_TYPE =
    "hbase.regionserver.wal.value.compression.type";

  public enum DictionaryIndex {
    REGION,
    TABLE,
    FAMILY,
    QUALIFIER,
    ROW
  }

  /**
   * Encapsulates the compression algorithm and its streams that we will use for value compression
   * in this WAL.
   */
  static class ValueCompressor {

    static final int IO_BUFFER_SIZE = 64 * 1024; // bigger buffer improves large edit compress ratio

    private final Compression.Algorithm algorithm;
    private Compressor compressor;
    private Decompressor decompressor;
    private WALDecompressionBoundedDelegatingInputStream lowerIn;
    private ByteArrayOutputStream lowerOut;
    private InputStream compressedIn;
    private OutputStream compressedOut;

    public ValueCompressor(Compression.Algorithm algorithm) {
      this.algorithm = algorithm;
    }

    public Compression.Algorithm getAlgorithm() {
      return algorithm;
    }

    public byte[] compress(byte[] valueArray, int valueOffset, int valueLength) throws IOException {
      if (compressedOut == null) {
        // Create the output streams here the first time around.
        lowerOut = new ByteArrayOutputStream();
        if (compressor == null) {
          compressor = algorithm.getCompressor();
        }
        compressedOut = algorithm.createCompressionStream(lowerOut, compressor, IO_BUFFER_SIZE);
      }
      compressedOut.write(valueArray, valueOffset, valueLength);
      compressedOut.flush();
      final byte[] compressed = lowerOut.toByteArray();
      lowerOut.reset(); // Reset now to minimize the overhead of keeping around the BAOS
      return compressed;
    }

    public void decompress(InputStream in, int inLength, byte[] outArray, int outOffset,
      int outLength) throws IOException {
      // Our input is a sequence of bounded byte ranges (call them segments), with
      // BoundedDelegatingInputStream providing a way to switch in a new segment when the
      // previous segment has been fully consumed.

      // Create the input streams here the first time around.
      if (compressedIn == null) {
        lowerIn = new WALDecompressionBoundedDelegatingInputStream();
        if (decompressor == null) {
          decompressor = algorithm.getDecompressor();
        }
        compressedIn = algorithm.createDecompressionStream(lowerIn, decompressor, IO_BUFFER_SIZE);
      }
      if (outLength == 0) {
        // The BufferedInputStream will return earlier and skip reading anything if outLength == 0,
        // but in fact for an empty value, the compressed output still contains some metadata so the
        // compressed size is not 0, so here we need to manually skip inLength bytes otherwise the
        // next read on this stream will start from an invalid position and cause critical problem,
        // such as data loss when splitting wal or replicating wal.
        IOUtils.skipFully(in, inLength);
      } else {
        lowerIn.reset(in, inLength);
        IOUtils.readFully(compressedIn, outArray, outOffset, outLength);
        // if the uncompressed size was larger than the configured buffer size for the codec,
        // the BlockCompressorStream will have left an extra 4 bytes hanging. This represents a size
        // for the next segment, and it should be 0. See HBASE-28390
        if (lowerIn.available() == 4) {
          int remaining = rawReadInt(lowerIn);
          assert remaining == 0;
        }
      }
    }

    /**
     * Read an integer from the stream in big-endian byte order.
     */
    private int rawReadInt(InputStream in) throws IOException {
      int b1 = in.read();
      int b2 = in.read();
      int b3 = in.read();
      int b4 = in.read();
      if ((b1 | b2 | b3 | b4) < 0) throw new EOFException();
      return ((b1 << 24) + (b2 << 16) + (b3 << 8) + (b4 << 0));
    }

    public void clear() {
      if (compressedOut != null) {
        try {
          compressedOut.close();
        } catch (IOException e) {
          LOG.warn("Exception closing compressed output stream", e);
        }
      }
      compressedOut = null;
      if (lowerOut != null) {
        try {
          lowerOut.close();
        } catch (IOException e) {
          LOG.warn("Exception closing lower output stream", e);
        }
      }
      lowerOut = null;
      if (compressedIn != null) {
        try {
          compressedIn.close();
        } catch (IOException e) {
          LOG.warn("Exception closing compressed input stream", e);
        }
      }
      compressedIn = null;
      if (lowerIn != null) {
        try {
          lowerIn.close();
        } catch (IOException e) {
          LOG.warn("Exception closing lower input stream", e);
        }
      }
      lowerIn = null;
      if (compressor != null) {
        compressor.reset();
      }
      if (decompressor != null) {
        decompressor.reset();
      }
    }

  }

  private final Map<DictionaryIndex, Dictionary> dictionaries =
    new EnumMap<>(DictionaryIndex.class);
  // Context used for compressing tags
  TagCompressionContext tagCompressionContext = null;
  ValueCompressor valueCompressor = null;

  public CompressionContext(Class<? extends Dictionary> dictType, boolean recoveredEdits,
    boolean hasTagCompression, boolean hasValueCompression,
    Compression.Algorithm valueCompressionType) throws SecurityException, NoSuchMethodException,
    InstantiationException, IllegalAccessException, InvocationTargetException, IOException {
    Constructor<? extends Dictionary> dictConstructor = dictType.getConstructor();
    for (DictionaryIndex dictionaryIndex : DictionaryIndex.values()) {
      Dictionary newDictionary = dictConstructor.newInstance();
      dictionaries.put(dictionaryIndex, newDictionary);
    }
    if (recoveredEdits) {
      getDictionary(DictionaryIndex.REGION).init(1);
      getDictionary(DictionaryIndex.TABLE).init(1);
    } else {
      getDictionary(DictionaryIndex.REGION).init(Short.MAX_VALUE);
      getDictionary(DictionaryIndex.TABLE).init(Short.MAX_VALUE);
    }

    getDictionary(DictionaryIndex.ROW).init(Short.MAX_VALUE);
    getDictionary(DictionaryIndex.FAMILY).init(Byte.MAX_VALUE);
    getDictionary(DictionaryIndex.QUALIFIER).init(Byte.MAX_VALUE);

    if (hasTagCompression) {
      tagCompressionContext = new TagCompressionContext(dictType, Short.MAX_VALUE);
    }
    if (hasValueCompression && valueCompressionType != null) {
      valueCompressor = new ValueCompressor(valueCompressionType);
    }
  }

  public CompressionContext(Class<? extends Dictionary> dictType, boolean recoveredEdits,
    boolean hasTagCompression) throws SecurityException, NoSuchMethodException,
    InstantiationException, IllegalAccessException, InvocationTargetException, IOException {
    this(dictType, recoveredEdits, hasTagCompression, false, null);
  }

  public boolean hasTagCompression() {
    return tagCompressionContext != null;
  }

  public boolean hasValueCompression() {
    return valueCompressor != null;
  }

  public Dictionary getDictionary(Enum dictIndex) {
    return dictionaries.get(dictIndex);
  }

  public ValueCompressor getValueCompressor() {
    return valueCompressor;
  }

  void clear() {
    for (Dictionary dictionary : dictionaries.values()) {
      dictionary.clear();
    }
    if (tagCompressionContext != null) {
      tagCompressionContext.clear();
    }
    if (valueCompressor != null) {
      valueCompressor.clear();
    }
  }

  public static Compression.Algorithm getValueCompressionAlgorithm(Configuration conf) {
    if (conf.getBoolean(ENABLE_WAL_VALUE_COMPRESSION, true)) {
      String compressionType = conf.get(WAL_VALUE_COMPRESSION_TYPE);
      if (compressionType != null) {
        return Compression.getCompressionAlgorithmByName(compressionType);
      }
      return Compression.Algorithm.GZ;
    }
    return Compression.Algorithm.NONE;
  }

}
