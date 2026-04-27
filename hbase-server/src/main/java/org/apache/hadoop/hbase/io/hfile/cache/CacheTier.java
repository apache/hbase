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
package org.apache.hadoop.hbase.io.hfile.cache;

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Logical cache tier used by topology and policy code.
 * <p>
 * This enum describes the role an engine plays inside a topology. It is intentionally separate from
 * {@link CacheEngineType}, which describes the implementation family of an engine.
 * </p>
 */
@InterfaceAudience.Private
public enum CacheTier {

  /**
   * Single-tier topology engine.
   */
  SINGLE,

  /**
   * First-level cache, usually smaller and optimized for low latency.
   */
  L1,

  /**
   * Second-level cache, usually larger and optimized for capacity.
   */
  L2
}
