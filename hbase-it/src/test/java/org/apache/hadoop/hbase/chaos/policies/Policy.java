/**
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

package org.apache.hadoop.hbase.chaos.policies;

import java.util.Properties;

import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.util.StoppableImplementation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A policy to introduce chaos to the cluster
 */
public abstract class Policy extends StoppableImplementation implements Runnable {

  protected static final Logger LOG = LoggerFactory.getLogger(Policy.class);

  protected PolicyContext context;

  public void init(PolicyContext context) throws Exception {
    this.context = context;

    // Used to wire up stopping.
    context.setPolicy(this);
  }

  /**
   * A context for a Policy
   */
  public static class PolicyContext extends Action.ActionContext {

    Policy policy = null;

    public PolicyContext(Properties monkeyProps, IntegrationTestingUtility util) {
      super(monkeyProps, util);
    }

    @Override
    public boolean isStopping() {
      return policy.isStopped();
    }

    public void setPolicy(Policy policy) {
      this.policy = policy;
    }
  }
}
