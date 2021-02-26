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
 * Thrown if a table should be enabled but is not.
 */
@InterfaceAudience.Public
public class TableNotEnabledException extends DoNotRetryIOException {
  private static final long serialVersionUID = 262144L;
  /** default constructor */
  public TableNotEnabledException() {
    super();
  }

  /**
   * @param tableName the name of table that is not enabled
   */
  public TableNotEnabledException(String tableName) {
    super(tableName);
  }

  /**
   * @param tableName the name of table that is not enabled
   */
  public TableNotEnabledException(TableName tableName) {
    this(tableName.getNameAsString());
  }

  /**
   * @param tableName the name of table that is not enabled
   */
  public TableNotEnabledException(byte[] tableName) {
    this(Bytes.toString(tableName));
  }
}
