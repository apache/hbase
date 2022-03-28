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
import java.net.InetAddress;
import java.util.Map;

import javax.security.sasl.SaslClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

/**
 * Encapsulation of client-side logic to authenticate to HBase via some means over SASL.
 * Implementations should not directly implement this interface, but instead extend
 * {@link AbstractSaslClientAuthenticationProvider}.
 *
 * Implementations of this interface must make an implementation of {@code hashCode()}
 * which returns the same value across multiple instances of the provider implementation.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public interface SaslClientAuthenticationProvider extends SaslAuthenticationProvider {

  /**
   * Creates the SASL client instance for this auth'n method.
   */
  SaslClient createClient(Configuration conf, InetAddress serverAddr, SecurityInfo securityInfo,
      Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException;

  /**
   * Constructs a {@link UserInformation} from the given {@link UserGroupInformation}
   */
  UserInformation getUserInfo(User user);

  /**
   * Returns the "real" user, the user who has the credentials being authenticated by the
   * remote service, in the form of an {@link UserGroupInformation} object.
   *
   * It is common in the Hadoop "world" to have distinct notions of a "real" user and a "proxy"
   * user. A "real" user is the user which actually has the credentials (often, a Kerberos ticket),
   * but some code may be running as some other user who has no credentials. This method gives
   * the authentication provider a chance to acknowledge this is happening and ensure that any
   * RPCs are executed with the real user's credentials, because executing them as the proxy user
   * would result in failure because no credentials exist to authenticate the RPC.
   *
   * Not all implementations will need to implement this method. By default, the provided User's
   * UGI is returned directly.
   */
  default UserGroupInformation getRealUser(User ugi) {
    return ugi.getUGI();
  }

  /**
   * Returns true if the implementation is capable of performing some action which may allow a
   * failed authentication to become a successful authentication. Otherwise, returns false
   */
  default boolean canRetry() {
    return false;
  }

  /**
   * Executes any necessary logic to re-login the client. Not all implementations will have
   * any logic that needs to be executed.
   */
  default void relogin() throws IOException {}
}
