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

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.io.TagCompressionContext;
import org.apache.hadoop.hbase.io.util.Dictionary;

/**
 * Context that holds the various dictionaries for compression in HLog.
 */
@InterfaceAudience.Private
class CompressionContext {

  static final String ENABLE_WAL_TAGS_COMPRESSION = 
      "hbase.regionserver.wal.tags.enablecompression";

  final Dictionary regionDict;
  final Dictionary tableDict;
  final Dictionary familyDict;
  final Dictionary qualifierDict;
  final Dictionary rowDict;
  // Context used for compressing tags
  TagCompressionContext tagCompressionContext = null;

  public CompressionContext(Class<? extends Dictionary> dictType, boolean recoveredEdits,
      boolean hasTagCompression) throws SecurityException, NoSuchMethodException,
      InstantiationException, IllegalAccessException, InvocationTargetException {
    Constructor<? extends Dictionary> dictConstructor =
        dictType.getConstructor();
    regionDict = dictConstructor.newInstance();
    tableDict = dictConstructor.newInstance();
    familyDict = dictConstructor.newInstance();
    qualifierDict = dictConstructor.newInstance();
    rowDict = dictConstructor.newInstance();
    if (recoveredEdits) {
      // This will never change
      regionDict.init(1);
      tableDict.init(1);
    } else {
      regionDict.init(Short.MAX_VALUE);
      tableDict.init(Short.MAX_VALUE);
    }
    rowDict.init(Short.MAX_VALUE);
    familyDict.init(Byte.MAX_VALUE);
    qualifierDict.init(Byte.MAX_VALUE);
    if (hasTagCompression) {
      tagCompressionContext = new TagCompressionContext(dictType, Short.MAX_VALUE);
    }
  }

  void clear() {
    regionDict.clear();
    tableDict.clear();
    familyDict.clear();
    qualifierDict.clear();
    rowDict.clear();
    if (tagCompressionContext != null) {
      tagCompressionContext.clear();
    }
  }
}
