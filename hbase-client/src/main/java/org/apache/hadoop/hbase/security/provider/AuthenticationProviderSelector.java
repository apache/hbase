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
package org.apache.hadoop.hbase.security.provider;

import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public interface AuthenticationProviderSelector {

  /**
   * Initializes the implementation with configuration and a set of providers available. This method
   * should be called exactly once per implementation prior to calling
   * {@link #selectProvider(String, User)}.
   */
  void configure(Configuration conf,
      Collection<SaslClientAuthenticationProvider> availableProviders);

  /**
   * Chooses the authentication provider which should be used given the provided client context
   * from the authentication providers passed in via {@link #configure(Configuration, Collection)}.
   */
  Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> selectProvider(
      String clusterId, User user);
}
