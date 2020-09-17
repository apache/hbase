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
package org.apache.hadoop.hbase.security;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.http.HttpConfig;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Strings;

@InterfaceAudience.Private
public final class HBaseKerberosUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HBaseKerberosUtils.class);

  public static final String KRB_PRINCIPAL = SecurityConstants.REGIONSERVER_KRB_PRINCIPAL;
  public static final String MASTER_KRB_PRINCIPAL = SecurityConstants.MASTER_KRB_PRINCIPAL;
  public static final String KRB_KEYTAB_FILE = SecurityConstants.REGIONSERVER_KRB_KEYTAB_FILE;
  public static final String CLIENT_PRINCIPAL = AuthUtil.HBASE_CLIENT_KERBEROS_PRINCIPAL;
  public static final String CLIENT_KEYTAB = AuthUtil.HBASE_CLIENT_KEYTAB_FILE;

  private HBaseKerberosUtils() {
  }

  public static boolean isKerberosPropertySetted() {
    String krbPrincipal = System.getProperty(KRB_PRINCIPAL);
    String krbKeytab = System.getProperty(KRB_KEYTAB_FILE);
    if (Strings.isNullOrEmpty(krbPrincipal) || Strings.isNullOrEmpty(krbKeytab)) {
      return false;
    }
    return true;
  }

  public static void setPrincipalForTesting(String principal) {
    setSystemProperty(KRB_PRINCIPAL, principal);
  }

  public static void setKeytabFileForTesting(String keytabFile) {
    setSystemProperty(KRB_KEYTAB_FILE, keytabFile);
  }

  public static void setClientPrincipalForTesting(String clientPrincipal) {
    setSystemProperty(CLIENT_PRINCIPAL, clientPrincipal);
  }

  public static void setClientKeytabForTesting(String clientKeytab) {
    setSystemProperty(CLIENT_KEYTAB, clientKeytab);
  }

  public static void setSystemProperty(String propertyName, String propertyValue) {
    System.setProperty(propertyName, propertyValue);
  }

  public static String getKeytabFileForTesting() {
    return System.getProperty(KRB_KEYTAB_FILE);
  }

  public static String getPrincipalForTesting() {
    return System.getProperty(KRB_PRINCIPAL);
  }

  public static String getClientPrincipalForTesting() {
    return System.getProperty(CLIENT_PRINCIPAL);
  }

  public static String getClientKeytabForTesting() {
    return System.getProperty(CLIENT_KEYTAB);
  }

  public static Configuration getConfigurationWoPrincipal() {
    Configuration conf = HBaseConfiguration.create();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
    return conf;
  }

  public static Configuration getSecuredConfiguration() {
    Configuration conf = HBaseConfiguration.create();
    setSecuredConfiguration(conf);
    return conf;
  }

  /**
   * Set up configuration for a secure HDFS+HBase cluster.
   * @param conf configuration object.
   * @param servicePrincipal service principal used by NN, HM and RS.
   * @param spnegoPrincipal SPNEGO principal used by NN web UI.
   */
  public static void setSecuredConfiguration(Configuration conf, String servicePrincipal,
    String spnegoPrincipal) {
    setPrincipalForTesting(servicePrincipal);
    setSecuredConfiguration(conf);
    setSecuredHadoopConfiguration(conf, spnegoPrincipal);
  }

  public static void setSecuredConfiguration(Configuration conf) {
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    conf.set(User.HBASE_SECURITY_CONF_KEY, "kerberos");
    conf.setBoolean(User.HBASE_SECURITY_AUTHORIZATION_CONF_KEY, true);
    conf.set(KRB_KEYTAB_FILE, System.getProperty(KRB_KEYTAB_FILE));
    conf.set(KRB_PRINCIPAL, System.getProperty(KRB_PRINCIPAL));
    conf.set(MASTER_KRB_PRINCIPAL, System.getProperty(KRB_PRINCIPAL));
  }

  private static void setSecuredHadoopConfiguration(Configuration conf,
    String spnegoServerPrincipal) {
    String serverPrincipal = System.getProperty(KRB_PRINCIPAL);
    String keytabFilePath = System.getProperty(KRB_KEYTAB_FILE);
    // HDFS
    conf.set(DFSConfigKeys.DFS_NAMENODE_KERBEROS_PRINCIPAL_KEY, serverPrincipal);
    conf.set(DFSConfigKeys.DFS_NAMENODE_KEYTAB_FILE_KEY, keytabFilePath);
    conf.set(DFSConfigKeys.DFS_DATANODE_KERBEROS_PRINCIPAL_KEY, serverPrincipal);
    conf.set(DFSConfigKeys.DFS_DATANODE_KEYTAB_FILE_KEY, keytabFilePath);
    conf.setBoolean(DFSConfigKeys.DFS_BLOCK_ACCESS_TOKEN_ENABLE_KEY, true);
    // YARN
    conf.set(YarnConfiguration.RM_PRINCIPAL, KRB_PRINCIPAL);
    conf.set(YarnConfiguration.NM_PRINCIPAL, KRB_PRINCIPAL);

    if (spnegoServerPrincipal != null) {
      conf.set(DFSConfigKeys.DFS_WEB_AUTHENTICATION_KERBEROS_PRINCIPAL_KEY, spnegoServerPrincipal);
    }

    conf.setBoolean("ignore.secure.ports.for.testing", true);

    UserGroupInformation.setConfiguration(conf);
  }

  /**
   * Set up SSL configuration for HDFS NameNode and DataNode.
   * @param utility a HBaseTestingUtility object.
   * @param clazz the caller test class.
   * @throws Exception if unable to set up SSL configuration
   */
  public static void setSSLConfiguration(HBaseCommonTestingUtility utility, Class<?> clazz)
    throws Exception {
    Configuration conf = utility.getConfiguration();
    conf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    conf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    conf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    File keystoresDir = new File(utility.getDataTestDir("keystore").toUri().getPath());
    keystoresDir.mkdirs();
    String sslConfDir = KeyStoreTestUtil.getClasspathDir(clazz);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir.getAbsolutePath(), sslConfDir, conf, false);
  }

  public static UserGroupInformation loginAndReturnUGI(Configuration conf, String username)
    throws IOException {
    String hostname = InetAddress.getLocalHost().getHostName();
    String keyTabFileConfKey = "hbase." + username + ".keytab.file";
    String keyTabFileLocation = conf.get(keyTabFileConfKey);
    String principalConfKey = "hbase." + username + ".kerberos.principal";
    String principal = org.apache.hadoop.security.SecurityUtil
      .getServerPrincipal(conf.get(principalConfKey), hostname);
    if (keyTabFileLocation == null || principal == null) {
      LOG.warn(
        "Principal or key tab file null for : " + principalConfKey + ", " + keyTabFileConfKey);
    }
    UserGroupInformation ugi =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabFileLocation);
    return ugi;
  }
}
