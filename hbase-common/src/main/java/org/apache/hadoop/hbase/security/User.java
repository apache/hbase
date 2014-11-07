/*
 *
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
import java.lang.reflect.UndeclaredThrowableException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Methods;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

/**
 * Wrapper to abstract out usage of user and group information in HBase.
 *
 * <p>
 * This class provides a common interface for interacting with user and group
 * information across changing APIs in different versions of Hadoop.  It only
 * provides access to the common set of functionality in
 * {@link org.apache.hadoop.security.UserGroupInformation} currently needed by
 * HBase, but can be extended as needs change.
 * </p>
 */
@InterfaceAudience.Private
public abstract class User {
  public static final String HBASE_SECURITY_CONF_KEY =
      "hbase.security.authentication";

  private static Log LOG = LogFactory.getLog(User.class);

  protected UserGroupInformation ugi;

  public UserGroupInformation getUGI() {
    return ugi;
  }

  /**
   * Returns the full user name.  For Kerberos principals this will include
   * the host and realm portions of the principal name.
   * @return User full name.
   */
  public String getName() {
    return ugi.getUserName();
  }

  /**
   * Returns the list of groups of which this user is a member.  On secure
   * Hadoop this returns the group information for the user as resolved on the
   * server.  For 0.20 based Hadoop, the group names are passed from the client.
   */
  public String[] getGroupNames() {
    return ugi.getGroupNames();
  }

  /**
   * Returns the shortened version of the user name -- the portion that maps
   * to an operating system user name.
   * @return Short name
   */
  public abstract String getShortName();

  /**
   * Executes the given action within the context of this user.
   */
  public abstract <T> T runAs(PrivilegedAction<T> action);

  /**
   * Executes the given action within the context of this user.
   */
  public abstract <T> T runAs(PrivilegedExceptionAction<T> action)
      throws IOException, InterruptedException;

  /**
   * Requests an authentication token for this user and stores it in the
   * user's credentials.
   *
   * @throws IOException
   */
  public abstract void obtainAuthTokenForJob(Configuration conf, Job job)
      throws IOException, InterruptedException;

  /**
   * Requests an authentication token for this user and stores it in the
   * user's credentials.
   *
   * @throws IOException
   */
  public abstract void obtainAuthTokenForJob(JobConf job)
      throws IOException, InterruptedException;

  /**
   * Returns the Token of the specified kind associated with this user,
   * or null if the Token is not present.
   *
   * @param kind the kind of token
   * @param service service on which the token is supposed to be used
   * @return the token of the specified kind.
   */
  public Token<?> getToken(String kind, String service) throws IOException {
    for (Token<?> token: ugi.getTokens()) {
      if (token.getKind().toString().equals(kind) &&
          (service != null && token.getService().toString().equals(service)))
      {
        return token;
      }
    }
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return ugi.equals(((User) o).ugi);
  }

  @Override
  public int hashCode() {
    return ugi.hashCode();
  }

  @Override
  public String toString() {
    return ugi.toString();
  }

  /**
   * Returns the {@code User} instance within current execution context.
   */
  public static User getCurrent() throws IOException {
    User user = new SecureHadoopUser();
    if (user.getUGI() == null) {
      return null;
    }
    return user;
  }

  /**
   * Executes the given action as the login user
   * @param action
   * @return
   * @throws IOException
   * @throws InterruptedException
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public static <T> T runAsLoginUser(PrivilegedExceptionAction<T> action) throws IOException {
    return doAsUser(UserGroupInformation.getLoginUser(), action);
  }

  private static <T> T doAsUser(UserGroupInformation ugi,
      PrivilegedExceptionAction<T> action) throws IOException {
    try {
      return ugi.doAs(action);
    } catch (InterruptedException ie) {
      throw new IOException(ie);
    }
  }

  /**
   * Wraps an underlying {@code UserGroupInformation} instance.
   * @param ugi The base Hadoop user
   * @return User
   */
  public static User create(UserGroupInformation ugi) {
    if (ugi == null) {
      return null;
    }
    return new SecureHadoopUser(ugi);
  }

  /**
   * Generates a new {@code User} instance specifically for use in test code.
   * @param name the full username
   * @param groups the group names to which the test user will belong
   * @return a new <code>User</code> instance
   */
  public static User createUserForTesting(Configuration conf,
      String name, String[] groups) {
    return SecureHadoopUser.createUserForTesting(conf, name, groups);
  }

  /**
   * Log in the current process using the given configuration keys for the
   * credential file and login principal.
   *
   * <p><strong>This is only applicable when
   * running on secure Hadoop</strong> -- see
   * org.apache.hadoop.security.SecurityUtil#login(Configuration,String,String,String).
   * On regular Hadoop (without security features), this will safely be ignored.
   * </p>
   *
   * @param conf The configuration data to use
   * @param fileConfKey Property key used to configure path to the credential file
   * @param principalConfKey Property key used to configure login principal
   * @param localhost Current hostname to use in any credentials
   * @throws IOException underlying exception from SecurityUtil.login() call
   */
  public static void login(Configuration conf, String fileConfKey,
      String principalConfKey, String localhost) throws IOException {
    SecureHadoopUser.login(conf, fileConfKey, principalConfKey, localhost);
  }

  /**
   * Returns whether or not Kerberos authentication is configured for Hadoop.
   * For non-secure Hadoop, this always returns <code>false</code>.
   * For secure Hadoop, it will return the value from
   * {@code UserGroupInformation.isSecurityEnabled()}.
   */
  public static boolean isSecurityEnabled() {
    return SecureHadoopUser.isSecurityEnabled();
  }

  /**
   * Returns whether or not secure authentication is enabled for HBase. Note that
   * HBase security requires HDFS security to provide any guarantees, so it is
   * recommended that secure HBase should run on secure HDFS.
   */
  public static boolean isHBaseSecurityEnabled(Configuration conf) {
    return "kerberos".equalsIgnoreCase(conf.get(HBASE_SECURITY_CONF_KEY));
  }

  /* Concrete implementations */

  /**
   * Bridges {@code User} invocations to underlying calls to
   * {@link org.apache.hadoop.security.UserGroupInformation} for secure Hadoop
   * 0.20 and versions 0.21 and above.
   */
  private static class SecureHadoopUser extends User {
    private String shortName;

    private SecureHadoopUser() throws IOException {
      ugi = UserGroupInformation.getCurrentUser();
    }

    private SecureHadoopUser(UserGroupInformation ugi) {
      this.ugi = ugi;
    }

    @Override
    public String getShortName() {
      if (shortName != null) return shortName;
      try {
        shortName = ugi.getShortUserName();
        return shortName;
      } catch (Exception e) {
        throw new RuntimeException("Unexpected error getting user short name",
          e);
      }
    }

    @Override
    public <T> T runAs(PrivilegedAction<T> action) {
      return ugi.doAs(action);
    }

    @Override
    public <T> T runAs(PrivilegedExceptionAction<T> action)
        throws IOException, InterruptedException {
      return ugi.doAs(action);
    }

    @Override
    public void obtainAuthTokenForJob(Configuration conf, Job job)
        throws IOException, InterruptedException {
      try {
        Class<?> c = Class.forName(
            "org.apache.hadoop.hbase.security.token.TokenUtil");
        Methods.call(c, null, "obtainTokenForJob",
            new Class[]{Configuration.class, UserGroupInformation.class,
                Job.class},
            new Object[]{conf, ugi, job});
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Failure loading TokenUtil class, "
            +"is secure RPC available?", cnfe);
      } catch (IOException ioe) {
        throw ioe;
      } catch (InterruptedException ie) {
        throw ie;
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected error calling TokenUtil.obtainAndCacheToken()");
      }
    }

    @Override
    public void obtainAuthTokenForJob(JobConf job)
        throws IOException, InterruptedException {
      try {
        Class<?> c = Class.forName(
            "org.apache.hadoop.hbase.security.token.TokenUtil");
        Methods.call(c, null, "obtainTokenForJob",
            new Class[]{JobConf.class, UserGroupInformation.class},
            new Object[]{job, ugi});
      } catch (ClassNotFoundException cnfe) {
        throw new RuntimeException("Failure loading TokenUtil class, "
            +"is secure RPC available?", cnfe);
      } catch (IOException ioe) {
        throw ioe;
      } catch (InterruptedException ie) {
        throw ie;
      } catch (RuntimeException re) {
        throw re;
      } catch (Exception e) {
        throw new UndeclaredThrowableException(e,
            "Unexpected error calling TokenUtil.obtainAndCacheToken()");
      }
    }

    /** @see User#createUserForTesting(org.apache.hadoop.conf.Configuration, String, String[]) */
    public static User createUserForTesting(Configuration conf,
        String name, String[] groups) {
      return new SecureHadoopUser(UserGroupInformation.createUserForTesting(name, groups));
    }

    /**
     * Obtain credentials for the current process using the configured
     * Kerberos keytab file and principal.
     * @see User#login(org.apache.hadoop.conf.Configuration, String, String, String)
     *
     * @param conf the Configuration to use
     * @param fileConfKey Configuration property key used to store the path
     * to the keytab file
     * @param principalConfKey Configuration property key used to store the
     * principal name to login as
     * @param localhost the local hostname
     */
    public static void login(Configuration conf, String fileConfKey,
        String principalConfKey, String localhost) throws IOException {
      if (isSecurityEnabled()) {
        SecurityUtil.login(conf, fileConfKey, principalConfKey, localhost);
      }
    }

    /**
     * Returns the result of {@code UserGroupInformation.isSecurityEnabled()}.
     */
    public static boolean isSecurityEnabled() {
      return UserGroupInformation.isSecurityEnabled();
    }
  }
}
