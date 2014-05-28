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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.IntegrationTestingUtility;
import org.apache.hadoop.hbase.chaos.actions.Action;
import org.apache.hadoop.hbase.util.StoppableImplementation;

/**
 * A policy to introduce chaos to the cluster
 */
public abstract class Policy extends StoppableImplementation implements Runnable {

  protected static Log LOG = LogFactory.getLog(Policy.class);

  protected PolicyContext context;

  public void init(PolicyContext context) throws Exception {
    this.context = context;
  }

  /**
   * A context for a Policy
   */
  public static class PolicyContext extends Action.ActionContext {
    public PolicyContext(IntegrationTestingUtility util) {
      super(util);
    }
  }
}
