/*
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
package org.apache.hadoop.hbase.quotas;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Describe the Throttle Type.
 */
@InterfaceAudience.Public
public enum ThrottleType {
  /** Throttling based on the number of requests per time-unit */
  REQUEST_NUMBER,

  /** Throttling based on the read+write data size */
  REQUEST_SIZE,

  /** Throttling based on the number of write requests per time-unit */
  WRITE_NUMBER,

  /** Throttling based on the write data size */
  WRITE_SIZE,

  /** Throttling based on the number of read requests per time-unit */
  READ_NUMBER,

  /** Throttling based on the read data size */
  READ_SIZE,

  /** Throttling based on the read+write capacity unit */
  REQUEST_CAPACITY_UNIT,

  /** Throttling based on the write data capacity unit */
  WRITE_CAPACITY_UNIT,

  /** Throttling based on the read data capacity unit */
  READ_CAPACITY_UNIT,
}
