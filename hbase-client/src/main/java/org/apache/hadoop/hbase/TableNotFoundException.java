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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/** Thrown when a table can not be located */
@InterfaceAudience.Public
public class TableNotFoundException extends DoNotRetryIOException {
  private static final long serialVersionUID = 993179627856392526L;

  /** default constructor */
  public TableNotFoundException() {
    super();
  }

  /** @param s message */
  public TableNotFoundException(String s) {
    super(s);
  }

  public TableNotFoundException(byte[] tableName) {
    super(Bytes.toString(tableName));
  }

  public TableNotFoundException(TableName tableName) {
    super(tableName.getNameAsString());
  }
}
