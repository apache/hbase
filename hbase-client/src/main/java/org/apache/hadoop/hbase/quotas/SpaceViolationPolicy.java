/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
 * Enumeration that represents the action HBase will take when a space quota is violated.
 *
 * The target for a violation policy is either an HBase table or namespace. In the case of a
 * namespace, it is treated as a collection of tables (all tables are subject to the same policy).
 */
@InterfaceAudience.Public
public enum SpaceViolationPolicy {
  /**
   * Disables the table(s).
   */
  DISABLE,
  /**
   * Disallows any mutations or compactions on the table(s).
   */
  NO_WRITES_COMPACTIONS,
  /**
   * Disallows any mutations (but allows compactions) on the table(s).
   */
  NO_WRITES,
  /**
   * Disallows any updates (but allows deletes and compactions) on the table(s).
   */
  NO_INSERTS
}
