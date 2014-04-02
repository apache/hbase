/**
 * Copyright 2014 The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;


/**
 * An immutable data structure storing a byte array and corresponding string
 * converted by {@code Bytes.toString(byte[])). The current encoding used {
 * @code Bytes} is UTF-8.
 *
 * NOTE this class is supposed to store a byte array of table name or column
 *      family name.
 *
 * {@ code StringBytes} is comparable and the hashCode and equals are based on
 * contents.
 */
public class StringBytes implements Comparable<StringBytes> {
  /**
   * The byte array value.
   */
  protected final byte[] bytes;
  /**
   * A string cache. Could be null.
   */
  protected volatile String string = null;

  /**
   * The computed hashCode.
   */
  protected final int hashCode;

  /**
   * Constructs a StringBytes from a string.
   *
   * @param string the non-null string value.
   */
  public StringBytes(String string) {
    if (string == null) {
      throw new IllegalArgumentException("string cannot be null");
    }

    this.string = string;
    this.bytes = Bytes.toBytes(string);
    this.hashCode = Bytes.hashCode(this.bytes);
  }

  /**
   * Constructs a StringBytes from a byte array.
   *
   * @param bytes the non-null byte array.
   */
  public StringBytes(byte[] bytes) {
    if (bytes == null) {
      throw new IllegalArgumentException("bytes cannot be null");
    }

    this.bytes = bytes;
    this.hashCode = Bytes.hashCode(this.bytes);
  }

  private void checkString() {
    if (this.string == null) {
      this.string = Bytes.toString(bytes);
    }
  }

  /**
   * @return the string value.
   */
  public String getString() {
    checkString();
    return this.string;
  }

  /**
   * @return the byte array value.
   */
  public byte[] getBytes() {
    return this.bytes;
  }

  @Override
  public int compareTo(StringBytes that) {
    return Bytes.BYTES_COMPARATOR.compare(this.bytes, that.bytes);
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object that) {
    if (this == that)
      return true;
    if (that == null)
      return false;
    if (!(that instanceof StringBytes))
      return false;
    StringBytes other = (StringBytes) that;
    if (this.hashCode != other.hashCode) {
      return false;
    }
    return Bytes.equals(bytes, other.bytes);
  }

  /**
   * @return whether another byte array has the same contents as this.
   */
  public boolean equalBytes(byte[] that) {
    return Bytes.equals(this.bytes, that);
  }

  /**
   * @return whether another string has the same contents as this.
   */
  public boolean equalString(String that) {
    return this.getString().equals(that);
  }

  @Override
  public String toString() {
    return this.getString();
  }

  /**
   * @return whether this StringBytes contains zero-length bytes.
   */
  public boolean isEmpty() {
    return bytes.length == 0;
  }
}
