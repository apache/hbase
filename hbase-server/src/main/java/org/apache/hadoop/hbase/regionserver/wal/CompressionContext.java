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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.EnumMap;
import java.util.Map;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.util.Dictionary;

/**
 * Context that holds the various dictionaries for compression in WAL.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
public class CompressionContext {

  public static final String ENABLE_WAL_TAGS_COMPRESSION =
    "hbase.regionserver.wal.tags.enablecompression";

  public static final String ENABLE_WAL_VALUE_COMPRESSION =
    "hbase.regionserver.wal.value.enablecompression";

  public enum DictionaryIndex {
    REGION, TABLE, FAMILY, QUALIFIER, ROW
  }

  /**
   * Encapsulates the zlib deflater/inflater pair we will use for value compression in this WAL.
   */
  static class ValueCompressor {
    final static int DEFAULT_DEFLATE_BUFFER_SIZE = 8*1024;
    final static int MAX_DEFLATE_BUFFER_SIZE = 256*1024;

    final Deflater deflater;
    final Inflater inflater;
    byte[] deflateBuffer;

    public ValueCompressor() {
      deflater = new Deflater();
      // Optimize for speed so we minimize the time spent writing the WAL. This still achieves
      // quite good results. (This is not really user serviceable.)
      deflater.setLevel(Deflater.BEST_SPEED);
      inflater = new Inflater();
    }

    public Deflater getDeflater() {
      return deflater;
    }

    public byte[] getDeflateBuffer() {
      if (deflateBuffer == null) {
        deflateBuffer = new byte[DEFAULT_DEFLATE_BUFFER_SIZE];
      }
      return deflateBuffer;
    }

    public int getDeflateBufferSize() {
      return deflateBuffer.length;
    }

    public void setDeflateBufferSize(int size) {
      if (size > MAX_DEFLATE_BUFFER_SIZE) {
        throw new IllegalArgumentException("Requested buffer size is too large, ask=" + size +
          ", max=" + MAX_DEFLATE_BUFFER_SIZE);
      }
      deflateBuffer = new byte[size];
    }

    public Inflater getInflater() {
      return inflater;
    }

    public void clear() {
      deflater.reset();
      inflater.reset();
      deflateBuffer = null;
    }

  };

  private final Map<DictionaryIndex, Dictionary> dictionaries =
      new EnumMap<>(DictionaryIndex.class);
  // Context used for compressing tags
  TagCompressionContext tagCompressionContext = null;
  ValueCompressor valueCompressor = null;

  public CompressionContext(Class<? extends Dictionary> dictType, boolean recoveredEdits,
      boolean hasTagCompression, boolean hasValueCompression)
      throws SecurityException, NoSuchMethodException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
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
    if (hasValueCompression) {
      valueCompressor = new ValueCompressor();
    }
  }

  public CompressionContext(Class<? extends Dictionary> dictType, boolean recoveredEdits,
      boolean hasTagCompression)
      throws SecurityException, NoSuchMethodException, InstantiationException,
        IllegalAccessException, InvocationTargetException {
    this(dictType, recoveredEdits, hasTagCompression, false);
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

}
