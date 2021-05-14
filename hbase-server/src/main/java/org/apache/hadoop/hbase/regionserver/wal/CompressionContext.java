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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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
import org.apache.hadoop.hbase.io.DelegatingInputStream;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.util.Dictionary;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Context that holds the various dictionaries for compression in WAL.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
public class CompressionContext {

  public static final String ENABLE_WAL_TAGS_COMPRESSION =
    "hbase.regionserver.wal.tags.enablecompression";

  public static final String ENABLE_WAL_VALUE_COMPRESSION =
    "hbase.regionserver.wal.value.enablecompression";

  public static final String WAL_VALUE_COMPRESSION_TYPE =
    "hbase.regionserver.wal.value.compression.type";

  public enum DictionaryIndex {
    REGION, TABLE, FAMILY, QUALIFIER, ROW
  }

  /**
   * Encapsulates the compression algorithm and its streams that we will use for value
   * compression in this WAL.
   */
  static class ValueCompressor {
  
    static final int IO_BUFFER_SIZE = 4096;

    private final Compression.Algorithm algorithm;
    private DelegatingInputStream lowerIn;
    private ByteArrayOutputStream lowerOut;
    private InputStream compressedIn;
    private OutputStream compressedOut;

    public ValueCompressor(Compression.Algorithm algorithm) throws IOException {
      this.algorithm = algorithm;
    }

    public Compression.Algorithm getAlgorithm() {
      return algorithm;
    }

    public byte[] compress(byte[] valueArray, int valueOffset, int valueLength)
        throws IOException {
      // We have to create the output streams here the first time around.
      if (compressedOut == null) {
        lowerOut = new ByteArrayOutputStream();
        compressedOut = algorithm.createCompressionStream(lowerOut, algorithm.getCompressor(),
          IO_BUFFER_SIZE);
      } else {
        lowerOut.reset();
      }
      compressedOut.write(valueArray, valueOffset, valueLength);
      compressedOut.flush();
      return lowerOut.toByteArray();
    }

    public int decompress(InputStream in, int inLength, byte[] outArray, int outOffset,
        int outLength) throws IOException {
      // Read all of the compressed bytes into a buffer.
      byte[] inBuffer = new byte[inLength];
      IOUtils.readFully(in, inBuffer);
      // We have to create the input streams here the first time around.
      if (compressedIn == null) {
        lowerIn = new DelegatingInputStream(new ByteArrayInputStream(inBuffer));
        compressedIn = algorithm.createDecompressionStream(lowerIn, algorithm.getDecompressor(),
          IO_BUFFER_SIZE);
      } else {
        lowerIn.setDelegate(new ByteArrayInputStream(inBuffer));
      }
      return compressedIn.read(outArray, outOffset, outLength);
    }

    public void clear() {
      lowerIn = null;
      compressedIn = null;
      lowerOut = null;
      compressedOut = null;
    }

  };

  private final Map<DictionaryIndex, Dictionary> dictionaries =
      new EnumMap<>(DictionaryIndex.class);
  // Context used for compressing tags
  TagCompressionContext tagCompressionContext = null;
  ValueCompressor valueCompressor = null;

  public CompressionContext(Class<? extends Dictionary> dictType,
      boolean recoveredEdits, boolean hasTagCompression, boolean hasValueCompression,
      Compression.Algorithm valueCompressionType)
      throws SecurityException, NoSuchMethodException, InstantiationException,
        IllegalAccessException, InvocationTargetException, IOException {
    Constructor<? extends Dictionary> dictConstructor =
        dictType.getConstructor();
    for (DictionaryIndex dictionaryIndex : DictionaryIndex.values()) {
      Dictionary newDictionary = dictConstructor.newInstance();
      dictionaries.put(dictionaryIndex, newDictionary);
    }
    if(recoveredEdits) {
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
      boolean hasTagCompression)
      throws SecurityException, NoSuchMethodException, InstantiationException,
        IllegalAccessException, InvocationTargetException, IOException {
    this(dictType, recoveredEdits, hasTagCompression, false, null);
  }

  public boolean hasTagCompression() {
    return tagCompressionContext != null;
  }

  public boolean hasValueCompression() {
    return valueCompressor != null;
  }

  public Dictionary getDictionary(Enum<DictionaryIndex> dictIndex) {
    return dictionaries.get(dictIndex);
  }

  public ValueCompressor getValueCompressor() {
    return valueCompressor;
  }

  void clear() {
    for(Dictionary dictionary : dictionaries.values()){
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
