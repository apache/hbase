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

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;

/**
 * Base implementation of {@link SaslClientAuthenticationProvider}. All implementations should extend
 * this class instead of directly implementing the interface.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.AUTHENTICATION)
@InterfaceStability.Evolving
public abstract class AbstractSaslClientAuthenticationProvider implements SaslClientAuthenticationProvider {
  public static final Text AUTH_TOKEN_TYPE = new Text("HBASE_AUTH_TOKEN");


  @Override
  public final Text getTokenKind() {
    // All HBase authentication tokens are "HBASE_AUTH_TOKEN"'s. We differentiate between them
    // via the code().
    return AUTH_TOKEN_TYPE;
  }

  /**
   * Provides a hash code to identify this AuthenticationProvider among others. These two fields must be
   * unique to ensure that authentication methods are clearly separated.
   */
  @Override
  public final int hashCode() {
    return getSaslAuthMethod().hashCode();
  }

  @Override
  public UserGroupInformation unwrapUgi(UserGroupInformation ugi) {
    // Unwrap the UGI with the real user when we're using Kerberos auth
    if (isKerberos() && ugi != null && ugi.getRealUser() != null) {
      return ugi.getRealUser();
    }

    // Otherwise, use the UGI we were given
    return ugi;
  }
}
