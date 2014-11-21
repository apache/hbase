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

import java.io.IOException;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.BaseConfigurable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Provide an instance of a user. Allows custom {@link User} creation.
 */
@InterfaceAudience.Private
public class UserProvider extends BaseConfigurable {

  private static final String USER_PROVIDER_CONF_KEY = "hbase.client.userprovider.class";

  /**
   * Instantiate the {@link UserProvider} specified in the configuration and set the passed
   * configuration via {@link UserProvider#setConf(Configuration)}
   * @param conf to read and set on the created {@link UserProvider}
   * @return a {@link UserProvider} ready for use.
   */
  public static UserProvider instantiate(Configuration conf) {
    Class<? extends UserProvider> clazz =
        conf.getClass(USER_PROVIDER_CONF_KEY, UserProvider.class, UserProvider.class);
    return ReflectionUtils.newInstance(clazz, conf);
  }

  /**
   * Set the {@link UserProvider} in the given configuration that should be instantiated
   * @param conf to update
   * @param provider class of the provider to set
   */
  public static void setUserProviderForTesting(Configuration conf,
      Class<? extends UserProvider> provider) {
    conf.set(USER_PROVIDER_CONF_KEY, provider.getName());
  }

  /**
   * @return the userName for the current logged-in user.
   * @throws IOException if the underlying user cannot be obtained
   */
  public String getCurrentUserName() throws IOException {
    User user = getCurrent();
    return user == null ? null : user.getName();
  }

  /**
   * @return <tt>true</tt> if security is enabled, <tt>false</tt> otherwise
   */
  public boolean isHBaseSecurityEnabled() {
    return User.isHBaseSecurityEnabled(this.getConf());
  }

  /**
   * @return whether or not Kerberos authentication is configured for Hadoop. For non-secure Hadoop,
   *         this always returns <code>false</code>. For secure Hadoop, it will return the value
   *         from {@code UserGroupInformation.isSecurityEnabled()}.
   */
  public boolean isHadoopSecurityEnabled() {
    return User.isSecurityEnabled();
  }

  /**
   * @return the current user within the current execution context
   * @throws IOException if the user cannot be loaded
   */
  public User getCurrent() throws IOException {
    return User.getCurrent();
  }

  /**
   * Wraps an underlying {@code UserGroupInformation} instance.
   * @param ugi The base Hadoop user
   * @return User
   */
  public User create(UserGroupInformation ugi) {
    return User.create(ugi);
  }

  /**
   * Log in the current process using the given configuration keys for the credential file and login
   * principal.
   * <p>
   * <strong>This is only applicable when running on secure Hadoop</strong> -- see
   * org.apache.hadoop.security.SecurityUtil#login(Configuration,String,String,String). On regular
   * Hadoop (without security features), this will safely be ignored.
   * </p>
   * @param fileConfKey Property key used to configure path to the credential file
   * @param principalConfKey Property key used to configure login principal
   * @param localhost Current hostname to use in any credentials
   * @throws IOException underlying exception from SecurityUtil.login() call
   */
  public void login(String fileConfKey, String principalConfKey, String localhost)
      throws IOException {
    User.login(getConf(), fileConfKey, principalConfKey, localhost);
  }
}
