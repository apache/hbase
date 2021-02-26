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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.util.Dictionary;

/**
 * Context that holds the various dictionaries for compression in WAL.
 */
@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
public class CompressionContext {

  static final String ENABLE_WAL_TAGS_COMPRESSION =
      "hbase.regionserver.wal.tags.enablecompression";

  public enum DictionaryIndex {
    REGION, TABLE, FAMILY, QUALIFIER, ROW
  }

  private final Map<DictionaryIndex, Dictionary> dictionaries =
      new EnumMap<>(DictionaryIndex.class);
  // Context used for compressing tags
  TagCompressionContext tagCompressionContext = null;

  public CompressionContext(Class<? extends Dictionary> dictType, boolean recoveredEdits,
      boolean hasTagCompression) throws SecurityException, NoSuchMethodException,
      InstantiationException, IllegalAccessException, InvocationTargetException {
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
  }

  public Dictionary getDictionary(Enum dictIndex) {
    return dictionaries.get(dictIndex);
  }

  void clear() {
    for(Dictionary dictionary : dictionaries.values()){
      dictionary.clear();
    }
    if (tagCompressionContext != null) {
      tagCompressionContext.clear();
    }
  }
}
