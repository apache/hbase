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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicy;
import org.apache.hadoop.hbase.quotas.SpaceViolationPolicyEnforcement;

/**
 * A {@link SpaceViolationPolicyEnforcement} implementation which disables all updates and
 * compactions. The enforcement counterpart to {@link SpaceViolationPolicy#NO_WRITES_COMPACTIONS}.
 */
@InterfaceAudience.Private
public class NoWritesCompactionsViolationPolicyEnforcement
    extends NoWritesViolationPolicyEnforcement {
  private static final Logger LOG = LoggerFactory.getLogger(
      NoWritesCompactionsViolationPolicyEnforcement.class);

  private AtomicBoolean disableCompactions = new AtomicBoolean(false);

  @Override
  public synchronized void enable() {
    boolean ret = disableCompactions.compareAndSet(false, true);
    if (!ret && LOG.isTraceEnabled()) {
      LOG.trace("Compactions were already disabled upon enabling the policy");
    }
  }

  @Override
  public synchronized void disable() {
    boolean ret = disableCompactions.compareAndSet(true, false);
    if (!ret && LOG.isTraceEnabled()) {
      LOG.trace("Compactions were already enabled upon disabling the policy");
    }
  }

  @Override
  public String getPolicyName() {
    return SpaceViolationPolicy.NO_WRITES_COMPACTIONS.name();
  }

  @Override
  public boolean areCompactionsDisabled() {
    return disableCompactions.get();
  }
}
