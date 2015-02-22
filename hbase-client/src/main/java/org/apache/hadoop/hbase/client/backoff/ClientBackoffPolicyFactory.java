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
package org.apache.hadoop.hbase.client.backoff;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.util.ReflectionUtils;

@InterfaceAudience.Private
@InterfaceStability.Evolving
public final class ClientBackoffPolicyFactory {

  private static final Log LOG = LogFactory.getLog(ClientBackoffPolicyFactory.class);

  private ClientBackoffPolicyFactory() {
  }

  public static ClientBackoffPolicy create(Configuration conf) {
    // create the backoff policy
    String className =
        conf.get(ClientBackoffPolicy.BACKOFF_POLICY_CLASS, NoBackoffPolicy.class
            .getName());
      return ReflectionUtils.instantiateWithCustomCtor(className,
          new Class<?>[] { Configuration.class }, new Object[] { conf });
  }

  /**
   * Default backoff policy that doesn't create any backoff for the client, regardless of load
   */
  public static class NoBackoffPolicy implements ClientBackoffPolicy {
    public NoBackoffPolicy(Configuration conf){
      // necessary to meet contract
    }

    @Override
    public long getBackoffTime(ServerName serverName, byte[] region, ServerStatistics stats) {
      return 0;
    }
  }
}