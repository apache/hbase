/**
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
package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown when a table has been modified concurrently
 */
@InterfaceAudience.Public
public class ConcurrentTableModificationException extends DoNotRetryIOException {
  private static final long serialVersionUID = 7453646730058600581L;

  /** default constructor */
  public ConcurrentTableModificationException() {
    super();
  }

  /**
   * Constructor
   * @param s message
   */
  public ConcurrentTableModificationException(String s) {
    super(s);
  }

  /**
   * @param tableName Name of table that is modified concurrently
   */
  public ConcurrentTableModificationException(byte[] tableName) {
    this(Bytes.toString(tableName));
  }

  /**
   * @param tableName Name of table that is modified concurrently
   */
  public ConcurrentTableModificationException(TableName tableName) {
    this(tableName.getNameAsString());
  }
}
