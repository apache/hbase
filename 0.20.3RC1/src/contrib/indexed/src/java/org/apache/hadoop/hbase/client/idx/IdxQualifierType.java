/**
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.client.idx;

/**
 * Indicates the data type contained in the column family qualifier.
 * This type affects index construction and value ordering in the index
 */
public enum IdxQualifierType {
  /**
   * Values qualified by this qualifier are bytes.
   * Each entry is a byte array of size 1 which should be treated as numerical
   * byte.
   */
  BYTE,
  /**
   * Values qualified by this qualifier are characters.
   * Each entry is a byte array of size 2 which should be treated as
   * a character.
   */
  CHAR,
  /**
   * Values qualified by this qualifier are short integers.
   * Each entry is a byte array of size 2 which should be treated as
   * a short integer.
   */
  SHORT,
  /**
   * Values qualified by this qualifier are integers.
   * Each entry is a byte array of size 4 which should be treated as
   * a an integer.
   */
  INT,
  /**
   * Values qualified by this qualifier are long integers.
   * Each entry is a byte array of size 8 which should be treated as
   * a long integer.
   */
  LONG,
  /**
   * Values qualified by this qualifier are floats.
   * Each entry is a byte array of size 4 which should be treated as
   * a float.
   */
  FLOAT,
  /**
   * Values qualified by this qualifier are doubles.
   * Each entry is a byte array of size 8 which should be treated as
   * a double.
   */
  DOUBLE,
  /**
   * Values qualified by this qualifier are big decimals.
   * Each entry is a byte array of variable size which should be treated as
   * a big decimal. See also conversion methods in
   * {@link org.apache.hadoop.hbase.util.Bytes}
   */
  BIG_DECIMAL,
  /**
   * Values qualified by this qualifier are byte arrays.
   * Each entry is a byte array of variable size which should be compared
   * based on the byte array's bytes numerical order.
   */
  BYTE_ARRAY,
  /**
   * Values qualified by this qualifier are character arrays.
   * Each entry is a byte array of variable size which should be compared
   * based on the char array's characters lexicographical order.
   */
  CHAR_ARRAY,
}
