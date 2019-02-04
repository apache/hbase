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
package org.apache.hadoop.hbase.quotas.policies;

import java.io.IOException;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;

/**
 * A {@link SpaceViolationPolicyEnforcement} which disables the table. The enforcement counterpart
 * to {@link SpaceViolationPolicy#DISABLE}. This violation policy is different from others as it
 * doesn't take action (i.e. enable/disable table) local to the RegionServer, like the other
 * ViolationPolicies do. In case of violation, the appropriate action is initiated by the master.
 */
@InterfaceAudience.Private
public class DisableTableViolationPolicyEnforcement extends DefaultViolationPolicyEnforcement {

  @Override
  public void enable() throws IOException {
    // do nothing
  }

  @Override
  public void disable() throws IOException {
    // do nothing
  }

  @Override
  public void check(Mutation m) throws SpaceLimitingException {
    // If this policy is enacted, then the table is (or should be) disabled.
    throw new SpaceLimitingException(
        getPolicyName(), "This table is disabled due to violating a space quota.");
  }

  @Override
  public String getPolicyName() {
    return SpaceViolationPolicy.DISABLE.name();
  }
}
