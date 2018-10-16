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
package org.apache.hadoop.hbase.security;

import org.apache.hadoop.hbase.security.UserProvider;

/**
 * A {@link UserProvider} that always says hadoop security is enabled, regardless of the underlying
 * configuration. HBase security is <i>not enabled</i> as this is used to determine if SASL is used
 * to do the authentication, which requires a Kerberos ticket (which we currently don't have in
 * tests).
 * <p>
 * This should only be used for <b>TESTING</b>.
 */
public class HadoopSecurityEnabledUserProviderForTesting extends UserProvider {

  @Override
  public boolean isHBaseSecurityEnabled() {
    return false;
  }

  @Override
  public boolean isHadoopSecurityEnabled() {
    return true;
  }
}
