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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Evolving
public interface AccessControlConstants {

  // Operation attributes for cell level security

  /** Cell level ACL */
  public static final String OP_ATTRIBUTE_ACL = "acl";
  /** Cell level ACL evaluation strategy */
  public static final String OP_ATTRIBUTE_ACL_STRATEGY = "acl.strategy";
  /** Default cell ACL evaluation strategy: Table and CF first, then ACL */
  public static final byte[] OP_ATTRIBUTE_ACL_STRATEGY_DEFAULT = new byte[] { 0 };
  /** Alternate cell ACL evaluation strategy: Cell ACL first, then table and CF */
  public static final byte[] OP_ATTRIBUTE_ACL_STRATEGY_CELL_FIRST = new byte[] { 1 };

}
