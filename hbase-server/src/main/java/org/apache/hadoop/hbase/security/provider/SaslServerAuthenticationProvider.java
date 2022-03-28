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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Encapsulates the server-side logic to authenticate a client over SASL. Tied one-to-one to
 * a single client authentication implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public interface SaslServerAuthenticationProvider extends SaslAuthenticationProvider {

  /**
   * Allows implementations to initialize themselves, prior to creating a server.
   */
  default void init(Configuration conf) throws IOException {}

  /**
   * Creates the SaslServer to accept incoming SASL authentication requests.
   */
  AttemptingUserProvidingSaslServer createServer(SecretManager<TokenIdentifier> secretManager,
      Map<String, String> saslProps) throws IOException;

  boolean supportsProtocolAuthentication();

  UserGroupInformation getAuthorizedUgi(String authzId,
      SecretManager<TokenIdentifier> secretManager) throws IOException;
}
