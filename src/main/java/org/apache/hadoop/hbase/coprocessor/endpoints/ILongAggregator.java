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
package org.apache.hadoop.hbase.coprocessor.endpoints;

import java.io.IOException;

/**
 * The endpoint that aggregates values.
 */
public interface ILongAggregator extends IEndpoint {
  /**
   * Sum of the values.
   *
   * @param family the column family name. All families if empty.
   * @param qualifier the column name. All columns if empty.
   * @param offset the offset of the long in the value in bytes.
   */
  public long sum(byte[] family, byte[] qualifier, int offset)
      throws IOException;

  /**
   * Min of the values. If no values found, Long.MAX_VALUE is returned.
   *
   * @param family the column family name. All families if empty.
   * @param qualifier the column name. All columns if empty.
   * @param offset the offset of the long in the value in bytes.
   */
  public long min(byte[] family, byte[] qualifier, int offset)
      throws IOException;

  /**
   * Max of the values.If no values found, Long.MIN_VALUE is returned.
   *
   * @param family the column family name. All families if empty.
   * @param qualifier the column name. All columns if empty.
   * @param offset the offset of the long in the value in bytes.
   */
  public long max(byte[] family, byte[] qualifier, int offset)
      throws IOException;
}
