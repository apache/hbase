/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * The interface represents any cell impl that is backed by contiguous layout like {@link KeyValue},
 * {@link ByteBufferKeyValue}. The APIs in this interface helps to easily access the internals of
 * the Cell structure so that we avoid fetching the offsets and lengths every time from the internal
 * backed memory structure. We can use this interface only if it follows the standard KeyValue
 * format. Helps in performance and this is jit friendly
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface ContiguousCellFormat {

  /**
   * Return the family length position/offset.
   * @param rowLen - the rowlength of the cell
   * @return the family's offset position
   */
  int getFamilyLengthPosition(int rowLen);

  /**
   * Return the family length. Use this along with {@link #getFamilyLengthPosition(int)}
   * @param famLengthPosition - the family offset
   * @return the family's length
   */
  byte getFamilyLength(int famLengthPosition);

  /**
   * Return the family position/offset. This can be used along with
   * the result of {@link #getFamilyLengthPosition(int)}
   * @param familyLengthPosition - the family length position
   * @return the family's position
   */
  int getFamilyInternalPosition(int familyLengthPosition);

  /**
   * Return the qualifier position/offset
   * @param famOffset - the family offset
   * @param famLength - the family length
   * @return the qualifier offset/position.
   */
  int getQualifierInternalPosition(int famOffset, byte famLength);

  /**
   * Return the qualifier length
   * @param keyLength - the key length
   * @param rowLen - the row length
   * @param famLength - the family length
   * @return the qualifier length
   */
  int getQualifierLength(int keyLength, int rowLen, int famLength);

  /**
   * Return the time stamp. Use this along with {@link #getKeyLength()}
   * @param keyLength - the key length
   * @return the timestamp
   */
  long getTimestamp(int keyLength);

  /**
   * Return the type byte. Use this along with {@link #getKeyLength()}
   * @param keyLength - the key length
   * @return - the type byte
   */
  byte getTypeByte(int keyLength);

  /**
   * The keylength of the cell
   * @return the key length
   */
  int getKeyLength();
}