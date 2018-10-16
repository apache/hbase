/*
 * Copyright The Apache Software Foundation
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

package org.apache.hadoop.hbase.client;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Specify Isolation levels in Scan operations.
 * <p>
 * There are two isolation levels. A READ_COMMITTED isolation level
 * indicates that only data that is committed be returned in a scan.
 * An isolation level of READ_UNCOMMITTED indicates that a scan
 * should return data that is being modified by transactions that might
 * not have been committed yet.
 */
@InterfaceAudience.Public
public enum IsolationLevel {

  READ_COMMITTED(1),
  READ_UNCOMMITTED(2);

  IsolationLevel(int value) {}

  public byte [] toBytes() {
    return new byte [] { toByte() };
  }

  public byte toByte() {
    return (byte)this.ordinal();
  }

  public static IsolationLevel fromBytes(byte [] bytes) {
    return IsolationLevel.fromByte(bytes[0]);
  }

  public static IsolationLevel fromByte(byte vbyte) {
    return IsolationLevel.values()[vbyte];
  }
}
