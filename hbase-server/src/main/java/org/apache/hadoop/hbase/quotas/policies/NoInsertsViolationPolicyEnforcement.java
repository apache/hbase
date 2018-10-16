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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.quotas.SpaceLimitingException;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;

/**
 * A {@link SpaceViolationPolicyEnforcement} which disallows any inserts to the table. The
 * enforcement counterpart to {@link SpaceViolationPolicy#NO_INSERTS}.
 */
@InterfaceAudience.Private
public class NoInsertsViolationPolicyEnforcement extends DefaultViolationPolicyEnforcement {

  @Override
  public void enable() {}

  @Override
  public void disable() {}

  @Override
  public void check(Mutation m) throws SpaceLimitingException {
    // Disallow all "new" data flowing into HBase, but allow Deletes (even though we know they will
    // temporarily increase utilization).
    if (m instanceof Append  || m instanceof Increment || m instanceof Put) {
      throw new SpaceLimitingException(getPolicyName(),
          m.getClass().getSimpleName() + "s are disallowed due to a space quota.");
    }
  }

  @Override
  public String getPolicyName() {
    return SpaceViolationPolicy.NO_INSERTS.name();
  }
}
