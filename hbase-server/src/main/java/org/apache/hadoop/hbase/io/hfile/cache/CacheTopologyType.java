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
 * Identifies the structural form of a cache topology.
 */
@InterfaceAudience.Private
public enum CacheTopologyType {

  /**
   * A topology with a single cache engine.
   */
  SINGLE,

  /**
   * A tiered topology where a block normally resides in only one tier at a time.
   */
  TIERED_EXCLUSIVE,

  /**
   * A tiered topology where a block present in an upper tier may also remain in a lower tier.
   */
  TIERED_INCLUSIVE
}
