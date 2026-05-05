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
 * Cached block representation kind.
 */
@InterfaceAudience.Private
public enum RepresentationKind {

  /**
   * Preserve the representation already produced by current HBase code paths.
   */
  CURRENT_HBASE_DEFAULT,

  /**
   * Prefer packed or serialized representation.
   */
  PACKED,

  /**
   * Prefer unpacked or ready-to-use representation.
   */
  UNPACKED,

  /**
   * Let the target cache engine choose its preferred representation.
   */
  ENGINE_DEFAULT
}
