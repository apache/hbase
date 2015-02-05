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

package org.apache.hadoop.hbase.security.access;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AccessControlConstants {

  /**
   * Configuration option that toggles whether EXEC permission checking is
   * performed during coprocessor endpoint invocations.
   */
  public static final String EXEC_PERMISSION_CHECKS_KEY = "hbase.security.exec.permission.checks";
  /** Default setting for hbase.security.exec.permission.checks; false */
  public static final boolean DEFAULT_EXEC_PERMISSION_CHECKS = false;

  /**
   * Configuration or CF schema option for early termination of access checks
   * if table or CF permissions grant access. Pre-0.98 compatible behavior
   */
  public static final String CF_ATTRIBUTE_EARLY_OUT = "hbase.security.access.early_out";
  /** Default setting for hbase.security.access.early_out; true*/
  public static final boolean DEFAULT_ATTRIBUTE_EARLY_OUT = true;

  // Operation attributes for cell level security

  /** Cell level ACL */
  @InterfaceStability.Unstable
  public static final String OP_ATTRIBUTE_ACL = "acl";
  /** Cell level ACL evaluation strategy */
  @InterfaceStability.Unstable
  public static final String OP_ATTRIBUTE_ACL_STRATEGY = "acl.strategy";
  /** Default cell ACL evaluation strategy: Table and CF first, then ACL */
  @InterfaceStability.Unstable
  public static final byte[] OP_ATTRIBUTE_ACL_STRATEGY_DEFAULT = new byte[] { 0 };
  /** Alternate cell ACL evaluation strategy: Cell ACL first, then table and CF */
  @InterfaceStability.Unstable
  public static final byte[] OP_ATTRIBUTE_ACL_STRATEGY_CELL_FIRST = new byte[] { 1 };

}
