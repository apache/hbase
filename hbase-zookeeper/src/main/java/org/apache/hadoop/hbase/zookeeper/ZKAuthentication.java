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

package org.apache.hadoop.hbase.zookeeper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.security.auth.login.AppConfigurationEntry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.server.ZooKeeperSaslServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides ZooKeeper authentication services for both client and server processes.
 */
@InterfaceAudience.Private
public final class ZKAuthentication {
  private static final Logger LOG = LoggerFactory.getLogger(ZKAuthentication.class);

  private ZKAuthentication() {}

  /**
   * Log in the current zookeeper server process using the given configuration
   * keys for the credential file and login principal.
   *
   * <p><strong>This is only applicable when running on secure hbase</strong>
   * On regular HBase (without security features), this will safely be ignored.
   * </p>
   *
   * @param conf The configuration data to use
   * @param keytabFileKey Property key used to configure the path to the credential file
   * @param userNameKey Property key used to configure the login principal
   * @param hostname Current hostname to use in any credentials
   * @throws IOException underlying exception from SecurityUtil.login() call
   */
  public static void loginServer(Configuration conf, String keytabFileKey,
      String userNameKey, String hostname) throws IOException {
    login(conf, keytabFileKey, userNameKey, hostname,
          ZooKeeperSaslServer.LOGIN_CONTEXT_NAME_KEY,
          JaasConfiguration.SERVER_KEYTAB_KERBEROS_CONFIG_NAME);
  }

  /**
   * Log in the current zookeeper client using the given configuration
   * keys for the credential file and login principal.
   *
   * <p><strong>This is only applicable when running on secure hbase</strong>
   * On regular HBase (without security features), this will safely be ignored.
   * </p>
   *
   * @param conf The configuration data to use
   * @param keytabFileKey Property key used to configure the path to the credential file
   * @param userNameKey Property key used to configure the login principal
   * @param hostname Current hostname to use in any credentials
   * @throws IOException underlying exception from SecurityUtil.login() call
   */
  public static void loginClient(Configuration conf, String keytabFileKey,
      String userNameKey, String hostname) throws IOException {
    login(conf, keytabFileKey, userNameKey, hostname,
          ZooKeeperSaslClient.LOGIN_CONTEXT_NAME_KEY,
          JaasConfiguration.CLIENT_KEYTAB_KERBEROS_CONFIG_NAME);
  }

  /**
   * Log in the current process using the given configuration keys for the
   * credential file and login principal.
   *
   * <p><strong>This is only applicable when running on secure hbase</strong>
   * On regular HBase (without security features), this will safely be ignored.
   * </p>
   *
   * @param conf The configuration data to use
   * @param keytabFileKey Property key used to configure the path to the credential file
   * @param userNameKey Property key used to configure the login principal
   * @param hostname Current hostname to use in any credentials
   * @param loginContextProperty property name to expose the entry name
   * @param loginContextName jaas entry name
   * @throws IOException underlying exception from SecurityUtil.login() call
   */
  private static void login(Configuration conf, String keytabFileKey,
      String userNameKey, String hostname,
      String loginContextProperty, String loginContextName)
      throws IOException {
    if (!isSecureZooKeeper(conf)) {
      return;
    }

    // User has specified a jaas.conf, keep this one as the good one.
    // HBASE_OPTS="-Djava.security.auth.login.config=jaas.conf"
    if (System.getProperty("java.security.auth.login.config") != null) {
      return;
    }

    // No keytab specified, no auth
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null) {
      LOG.warn("no keytab specified for: {}", keytabFileKey);
      return;
    }

    String principalConfig = conf.get(userNameKey, System.getProperty("user.name"));
    String principalName = SecurityUtil.getServerPrincipal(principalConfig, hostname);

    // Initialize the "jaas.conf" for keyTab/principal,
    // If keyTab is not specified use the Ticket Cache.
    // and set the zookeeper login context name.
    JaasConfiguration jaasConf = new JaasConfiguration(loginContextName,
        principalName, keytabFilename);
    javax.security.auth.login.Configuration.setConfiguration(jaasConf);
    System.setProperty(loginContextProperty, loginContextName);
  }

  /**
   * Returns {@code true} when secure authentication is enabled
   * (whether {@code hbase.security.authentication} is set to
   * "{@code kerberos}").
   */
  public static boolean isSecureZooKeeper(Configuration conf) {
    // Detection for embedded HBase client with jaas configuration
    // defined for third party programs.
    try {
      javax.security.auth.login.Configuration testConfig =
          javax.security.auth.login.Configuration.getConfiguration();
      if (testConfig.getAppConfigurationEntry("Client") == null
          && testConfig.getAppConfigurationEntry(
            JaasConfiguration.CLIENT_KEYTAB_KERBEROS_CONFIG_NAME) == null
          && testConfig.getAppConfigurationEntry(
              JaasConfiguration.SERVER_KEYTAB_KERBEROS_CONFIG_NAME) == null
          && conf.get(HConstants.ZK_CLIENT_KERBEROS_PRINCIPAL) == null
          && conf.get(HConstants.ZK_SERVER_KERBEROS_PRINCIPAL) == null) {

        return false;
      }
    } catch(Exception e) {
      // No Jaas configuration defined.
      return false;
    }

    // Master & RSs uses hbase.zookeeper.client.*
    return "kerberos".equalsIgnoreCase(conf.get("hbase.security.authentication"));
  }

  /**
   * A JAAS configuration that defines the login modules that we want to use for ZooKeeper login.
   */
  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    private static final Logger LOG = LoggerFactory.getLogger(JaasConfiguration.class);

    public static final String SERVER_KEYTAB_KERBEROS_CONFIG_NAME =
      "zookeeper-server-keytab-kerberos";
    public static final String CLIENT_KEYTAB_KERBEROS_CONFIG_NAME =
      "zookeeper-client-keytab-kerberos";

    private static final Map<String, String> BASIC_JAAS_OPTIONS = new HashMap<>();

    static {
      String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
      if ("true".equalsIgnoreCase(jaasEnvVar)) {
        BASIC_JAAS_OPTIONS.put("debug", "true");
      }
    }

    private static final Map<String, String> KEYTAB_KERBEROS_OPTIONS = new HashMap<>();

    static {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
      KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
      KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
    }

    private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
      new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, KEYTAB_KERBEROS_OPTIONS);

    private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
      new AppConfigurationEntry[] { KEYTAB_KERBEROS_LOGIN };

    private javax.security.auth.login.Configuration baseConfig;
    private final String loginContextName;
    private final boolean useTicketCache;
    private final String keytabFile;
    private final String principal;

    public JaasConfiguration(String loginContextName, String principal, String keytabFile) {
      this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
    }

    private JaasConfiguration(String loginContextName, String principal, String keytabFile,
      boolean useTicketCache) {
      try {
        this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
      } catch (SecurityException e) {
        this.baseConfig = null;
      }
      this.loginContextName = loginContextName;
      this.useTicketCache = useTicketCache;
      this.keytabFile = keytabFile;
      this.principal = principal;
      LOG.info(
        "JaasConfiguration loginContextName={} principal={} useTicketCache={} keytabFile={}",
        loginContextName, principal, useTicketCache, keytabFile);
    }

    @Override public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        if (!useTicketCache) {
          KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
          KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
        }
        KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
        KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", useTicketCache ? "true" : "false");
        return KEYTAB_KERBEROS_CONF;
      }

      if (baseConfig != null) {
        return baseConfig.getAppConfigurationEntry(appName);
      }

      return (null);
    }
  }
}
