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
package org.apache.hadoop.hbase.snapshot;

import java.io.IOException;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * Thrown if a table should be online/offline, but is partially open.
 */
@InterfaceAudience.Public
public class TablePartiallyOpenException extends IOException {
  private static final long serialVersionUID = 3571982660065058361L;

  /**
   * Constructs an {@code TablePartiallyOpenException} with {@code null} as its error detail
   * message.
   */
  public TablePartiallyOpenException() {
    super();
  }

  /**
   * @param message the message describing the exception
   */
  public TablePartiallyOpenException(String message) {
    super(message);
  }

  /**
   * @param tableName the name of the table that is partially open
   */
  public TablePartiallyOpenException(TableName tableName) {
    this(tableName.getNameAsString());
  }

  /**
   * @param tableName the name of the table that is partially open
   */
  public TablePartiallyOpenException(byte[] tableName) {
    this(Bytes.toString(tableName));
  }
}
