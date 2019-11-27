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

import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Encapsulation of client-side logic to authenticate to HBase via some means over SASL.
 * Implementations should not directly implement this interface, but instead extend
 * {@link AbstractSaslClientAuthenticationProvider}.
 *
 * Implementations of this interface must make an implementation of {link {@link #hashCode()}
 * which returns the same value across multiple instances of the provider implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public interface SaslClientAuthenticationProvider {

  /**
   * Creates the SASL client instance for this auth'n method.
   */
  SaslClient createClient(Configuration conf, String serverPrincipal,
      Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException;

  /**
   * Returns the attributes which identify how this provider authenticates.
   */
  SaslAuthMethod getSaslAuthMethod();

  /**
   * Returns the name of the type used by the TokenIdentifier.
   */
  Text getTokenKind();

  /**
   * Constructs a {@link UserInformation} from the given {@link UserGroupInformation}
   */
  UserInformation getUserInfo(UserGroupInformation user);

  /**
   * Returns true if this provider is based on Kerberos. False, otherwise.
   */
  boolean isKerberos();

  /**
   * Performs any necessary unwrapping of a "proxy" user UGI which is executing some request
   * on top of a "real" user. This operation may simply return the original UGI if that is
   * what is appropriate for the given implementation.
   */
  UserGroupInformation unwrapUgi(UserGroupInformation ugi);
}
