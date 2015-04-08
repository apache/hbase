/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.quotas;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;

/**
 * Describe the Scope of the quota rules. The quota can be enforced at the cluster level or at
 * machine level.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public enum QuotaScope {
  /**
   * The specified throttling rules will be applied at the cluster level. A limit of 100req/min
   * means 100req/min in total. If you execute 50req on a machine and then 50req on another machine
   * then you have to wait your quota to fill up.
   */
  CLUSTER,

  /**
   * The specified throttling rules will be applied on the machine level. A limit of 100req/min
   * means that each machine can execute 100req/min.
   */
  MACHINE,
}
