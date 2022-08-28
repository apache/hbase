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
package org.apache.hadoop.hbase.security.auth;

import java.util.Map;
import javax.security.auth.callback.CallbackHandler;
import org.apache.hadoop.conf.Configuration;
import org.apache.yetus.audience.InterfaceAudience;

/*
 * Callback handler for SASL-based authentication
 */
@InterfaceAudience.Private
public interface AuthenticateCallbackHandler extends CallbackHandler {

  /**
   * Configures this callback handler for the specified SASL mechanism.
   * @param configs       Key-value pairs containing the parsed configuration options of the client
   *                      or server. Note that these are the HBase configuration options and not the
   *                      JAAS configuration options. JAAS config options may be obtained from
   *                      `jaasConfigEntries` for callbacks which obtain some configs from the JAAS
   *                      configuration. For configs that may be specified as both HBase config as
   *                      well as JAAS config (e.g. sasl.kerberos.service.name), the configuration
   *                      is treated as invalid if conflicting values are provided.
   * @param saslMechanism Negotiated SASL mechanism. For clients, this is the SASL mechanism
   *                      configured for the client. For brokers, this is the mechanism negotiated
   *                      with the client and is one of the mechanisms enabled on the broker.
   * @param saslProps     SASL properties provided by the SASL library.
   */
  default void configure(Configuration configs, String saslMechanism,
    Map<String, String> saslProps) {
  }
}
