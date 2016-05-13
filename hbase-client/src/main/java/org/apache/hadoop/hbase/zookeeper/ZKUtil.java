/**
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
package org.apache.hadoop.hbase.zookeeper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.AppConfigurationEntry.LoginModuleControlFlag;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClusterStatusProtos.RegionStoreSequenceIds;
import org.apache.hadoop.hbase.protobuf.generated.ZooKeeperProtos;
import org.apache.hadoop.hbase.replication.ReplicationStateZKBase;
import org.apache.hadoop.hbase.security.Superusers;
import org.apache.hadoop.hbase.util.ByteStringer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp.CreateAndFailSilent;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp.DeleteNodeFailSilent;
import org.apache.hadoop.hbase.zookeeper.ZKUtil.ZKUtilOp.SetData;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooDefs.Perms;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.client.ZooKeeperSaslClient;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.proto.CreateRequest;
import org.apache.zookeeper.proto.DeleteRequest;
import org.apache.zookeeper.proto.SetDataRequest;
import org.apache.zookeeper.server.ZooKeeperSaslServer;

import com.google.protobuf.InvalidProtocolBufferException;

/**
 * Internal HBase utility class for ZooKeeper.
 *
 * <p>Contains only static methods and constants.
 *
 * <p>Methods all throw {@link KeeperException} if there is an unexpected
 * zookeeper exception, so callers of these methods must handle appropriately.
 * If ZK is required for the operation, the server will need to be aborted.
 */
@InterfaceAudience.Private
public class ZKUtil {
  private static final Log LOG = LogFactory.getLog(ZKUtil.class);

  // TODO: Replace this with ZooKeeper constant when ZOOKEEPER-277 is resolved.
  public static final char ZNODE_PATH_SEPARATOR = '/';
  private static int zkDumpConnectionTimeOut;

  /**
   * Creates a new connection to ZooKeeper, pulling settings and ensemble config
   * from the specified configuration object using methods from {@link ZKConfig}.
   *
   * Sets the connection status monitoring watcher to the specified watcher.
   *
   * @param conf configuration to pull ensemble and other settings from
   * @param watcher watcher to monitor connection changes
   * @return connection to zookeeper
   * @throws IOException if unable to connect to zk or config problem
   */
  public static RecoverableZooKeeper connect(Configuration conf, Watcher watcher)
  throws IOException {
    String ensemble = ZKConfig.getZKQuorumServersString(conf);
    return connect(conf, ensemble, watcher);
  }

  public static RecoverableZooKeeper connect(Configuration conf, String ensemble,
      Watcher watcher)
  throws IOException {
    return connect(conf, ensemble, watcher, null);
  }

  public static RecoverableZooKeeper connect(Configuration conf, String ensemble,
      Watcher watcher, final String identifier)
  throws IOException {
    if(ensemble == null) {
      throw new IOException("Unable to determine ZooKeeper ensemble");
    }
    int timeout = conf.getInt(HConstants.ZK_SESSION_TIMEOUT,
        HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
    if (LOG.isTraceEnabled()) {
      LOG.trace(identifier + " opening connection to ZooKeeper ensemble=" + ensemble);
    }
    int retry = conf.getInt("zookeeper.recovery.retry", 3);
    int retryIntervalMillis =
      conf.getInt("zookeeper.recovery.retry.intervalmill", 1000);
    zkDumpConnectionTimeOut = conf.getInt("zookeeper.dump.connection.timeout",
        1000);
    return new RecoverableZooKeeper(ensemble, timeout, watcher,
        retry, retryIntervalMillis, identifier);
  }

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
    if (!isSecureZooKeeper(conf))
      return;

    // User has specified a jaas.conf, keep this one as the good one.
    // HBASE_OPTS="-Djava.security.auth.login.config=jaas.conf"
    if (System.getProperty("java.security.auth.login.config") != null)
      return;

    // No keytab specified, no auth
    String keytabFilename = conf.get(keytabFileKey);
    if (keytabFilename == null) {
      LOG.warn("no keytab specified for: " + keytabFileKey);
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
   * A JAAS configuration that defines the login modules that we want to use for login.
   */
  private static class JaasConfiguration extends javax.security.auth.login.Configuration {
    private static final String SERVER_KEYTAB_KERBEROS_CONFIG_NAME =
      "zookeeper-server-keytab-kerberos";
    private static final String CLIENT_KEYTAB_KERBEROS_CONFIG_NAME =
      "zookeeper-client-keytab-kerberos";

    private static final Map<String, String> BASIC_JAAS_OPTIONS =
      new HashMap<String,String>();
    static {
      String jaasEnvVar = System.getenv("HBASE_JAAS_DEBUG");
      if (jaasEnvVar != null && "true".equalsIgnoreCase(jaasEnvVar)) {
        BASIC_JAAS_OPTIONS.put("debug", "true");
      }
    }

    private static final Map<String,String> KEYTAB_KERBEROS_OPTIONS =
      new HashMap<String,String>();
    static {
      KEYTAB_KERBEROS_OPTIONS.put("doNotPrompt", "true");
      KEYTAB_KERBEROS_OPTIONS.put("storeKey", "true");
      KEYTAB_KERBEROS_OPTIONS.put("refreshKrb5Config", "true");
      KEYTAB_KERBEROS_OPTIONS.putAll(BASIC_JAAS_OPTIONS);
    }

    private static final AppConfigurationEntry KEYTAB_KERBEROS_LOGIN =
      new AppConfigurationEntry(KerberosUtil.getKrb5LoginModuleName(),
                                LoginModuleControlFlag.REQUIRED,
                                KEYTAB_KERBEROS_OPTIONS);

    private static final AppConfigurationEntry[] KEYTAB_KERBEROS_CONF =
      new AppConfigurationEntry[]{KEYTAB_KERBEROS_LOGIN};

    private javax.security.auth.login.Configuration baseConfig;
    private final String loginContextName;
    private final boolean useTicketCache;
    private final String keytabFile;
    private final String principal;

    public JaasConfiguration(String loginContextName, String principal, String keytabFile) {
      this(loginContextName, principal, keytabFile, keytabFile == null || keytabFile.length() == 0);
    }

    private JaasConfiguration(String loginContextName, String principal,
                             String keytabFile, boolean useTicketCache) {
      try {
        this.baseConfig = javax.security.auth.login.Configuration.getConfiguration();
      } catch (SecurityException e) {
        this.baseConfig = null;
      }
      this.loginContextName = loginContextName;
      this.useTicketCache = useTicketCache;
      this.keytabFile = keytabFile;
      this.principal = principal;
      LOG.info("JaasConfiguration loginContextName=" + loginContextName +
               " principal=" + principal + " useTicketCache=" + useTicketCache +
               " keytabFile=" + keytabFile);
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String appName) {
      if (loginContextName.equals(appName)) {
        if (!useTicketCache) {
          KEYTAB_KERBEROS_OPTIONS.put("keyTab", keytabFile);
          KEYTAB_KERBEROS_OPTIONS.put("useKeyTab", "true");
        }
        KEYTAB_KERBEROS_OPTIONS.put("principal", principal);
        KEYTAB_KERBEROS_OPTIONS.put("useTicketCache", useTicketCache ? "true" : "false");
        return KEYTAB_KERBEROS_CONF;
      }
      if (baseConfig != null) return baseConfig.getAppConfigurationEntry(appName);
      return(null);
    }
  }

  //
  // Helper methods
  //

  /**
   * Join the prefix znode name with the suffix znode name to generate a proper
   * full znode name.
   *
   * Assumes prefix does not end with slash and suffix does not begin with it.
   *
   * @param prefix beginning of znode name
   * @param suffix ending of znode name
   * @return result of properly joining prefix with suffix
   */
  public static String joinZNode(String prefix, String suffix) {
    return prefix + ZNODE_PATH_SEPARATOR + suffix;
  }

  /**
   * Returns the full path of the immediate parent of the specified node.
   * @param node path to get parent of
   * @return parent of path, null if passed the root node or an invalid node
   */
  public static String getParent(String node) {
    int idx = node.lastIndexOf(ZNODE_PATH_SEPARATOR);
    return idx <= 0 ? null : node.substring(0, idx);
  }

  /**
   * Get the name of the current node from the specified fully-qualified path.
   * @param path fully-qualified path
   * @return name of the current node
   */
  public static String getNodeName(String path) {
    return path.substring(path.lastIndexOf("/")+1);
  }

  //
  // Existence checks and watches
  //

  /**
   * Watch the specified znode for delete/create/change events.  The watcher is
   * set whether or not the node exists.  If the node already exists, the method
   * returns true.  If the node does not exist, the method returns false.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if znode exists, false if does not exist or error
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean watchAndCheckExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat s = zkw.getRecoverableZooKeeper().exists(znode, zkw);
      boolean exists = s != null ? true : false;
      if (exists) {
        LOG.debug(zkw.prefix("Set watcher on existing znode=" + znode));
      } else {
        LOG.debug(zkw.prefix("Set watcher on znode that does not yet exist, " + znode));
      }
      return exists;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Watch the specified znode, but only if exists. Useful when watching
   * for deletions. Uses .getData() (and handles NoNodeException) instead
   * of .exists() to accomplish this, as .getData() will only set a watch if
   * the znode exists.
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return true if the watch is set, false if node does not exists
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setWatchIfNodeExists(ZooKeeperWatcher zkw, String znode)
      throws KeeperException {
    try {
      zkw.getRecoverableZooKeeper().getData(znode, true, null);
      return true;
    } catch (NoNodeException e) {
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Check if the specified node exists.  Sets no watches.
   *
   * @param zkw zk reference
   * @param znode path of node to watch
   * @return version of the node if it exists, -1 if does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int checkExists(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat s = zkw.getRecoverableZooKeeper().exists(znode, null);
      return s != null ? s.getVersion() : -1;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
      zkw.keeperException(e);
      return -1;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to set watcher on znode (" + znode + ")"), e);
      zkw.interruptedException(e);
      return -1;
    }
  }

  //
  // Znode listings
  //

  /**
   * Lists the children znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param znode path of node to list and watch children of
   * @return list of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenAndWatchForNewChildren(
      ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      List<String> children = zkw.getRecoverableZooKeeper().getChildren(znode, zkw);
      return children;
    } catch(KeeperException.NoNodeException ke) {
      LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
          "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode + " "), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * List all the children of the specified znode, setting a watch for children
   * changes and also setting a watch on every individual child in order to get
   * the NodeCreated and NodeDeleted events.
   * @param zkw zookeeper reference
   * @param znode node to get children of and watch
   * @return list of znode names, null if the node doesn't exist
   * @throws KeeperException
   */
  public static List<String> listChildrenAndWatchThem(ZooKeeperWatcher zkw,
      String znode) throws KeeperException {
    List<String> children = listChildrenAndWatchForNewChildren(zkw, znode);
    if (children == null) {
      return null;
    }
    for (String child : children) {
      watchAndCheckExists(zkw, joinZNode(znode, child));
    }
    return children;
  }

  /**
   * Lists the children of the specified znode without setting any watches.
   *
   * Sets no watches at all, this method is best effort.
   *
   * Returns an empty list if the node has no children.  Returns null if the
   * parent node itself does not exist.
   *
   * @param zkw zookeeper reference
   * @param znode node to get children
   * @return list of data of children of specified znode, empty if no children,
   *         null if parent does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static List<String> listChildrenNoWatch(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    List<String> children = null;
    try {
      // List the children without watching
      children = zkw.getRecoverableZooKeeper().getChildren(znode, null);
    } catch(KeeperException.NoNodeException nne) {
      return null;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
    return children;
  }

  /**
   * Simple class to hold a node path and node data.
   * @deprecated Unused
   */
  @Deprecated
  public static class NodeAndData {
    private String node;
    private byte [] data;
    public NodeAndData(String node, byte [] data) {
      this.node = node;
      this.data = data;
    }
    public String getNode() {
      return node;
    }
    public byte [] getData() {
      return data;
    }
    @Override
    public String toString() {
      return node;
    }
    public boolean isEmpty() {
      return (data == null || data.length == 0);
    }
  }

  /**
   * Checks if the specified znode has any children.  Sets no watches.
   *
   * Returns true if the node exists and has children.  Returns false if the
   * node does not exist or if the node does not have any children.
   *
   * Used during master initialization to determine if the master is a
   * failed-over-to master or the first master during initial cluster startup.
   * If the directory for regionserver ephemeral nodes is empty then this is
   * a cluster startup, if not then it is not cluster startup.
   *
   * @param zkw zk reference
   * @param znode path of node to check for children of
   * @return true if node has children, false if not or node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean nodeHasChildren(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      return !zkw.getRecoverableZooKeeper().getChildren(znode, null).isEmpty();
    } catch(KeeperException.NoNodeException ke) {
      LOG.debug(zkw.prefix("Unable to list children of znode " + znode + " " +
      "because node does not exist (not an error)"));
      return false;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
      zkw.keeperException(e);
      return false;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to list children of znode " + znode), e);
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Get the number of children of the specified node.
   *
   * If the node does not exist or has no children, returns 0.
   *
   * Sets no watches at all.
   *
   * @param zkw zk reference
   * @param znode path of node to count children of
   * @return number of children of specified node, 0 if none or parent does not
   *         exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static int getNumberOfChildren(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    try {
      Stat stat = zkw.getRecoverableZooKeeper().exists(znode, null);
      return stat == null ? 0 : stat.getNumChildren();
    } catch(KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get children of node " + znode));
      zkw.keeperException(e);
    } catch(InterruptedException e) {
      zkw.interruptedException(e);
    }
    return 0;
  }

  //
  // Data retrieval
  //

  /**
   * Get znode data. Does not set a watcher.
   * @return ZNode data, null if the node does not exist or if there is an
   *  error.
   */
  public static byte [] getData(ZooKeeperWatcher zkw, String znode)
      throws KeeperException, InterruptedException {
    try {
      byte [] data = zkw.getRecoverableZooKeeper().getData(znode, null, null);
      logRetrievedMsg(zkw, znode, data, false);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
          "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    }
  }

  /**
   * Get the data at the specified znode and set a watch.
   *
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataAndWatch(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    return getDataInternal(zkw, znode, null, true);
  }

  /**
   * Get the data at the specified znode and set a watch.
   *
   * Returns the data and sets a watch if the node exists.  Returns null and no
   * watch is set if the node does not exist or there is an exception.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param stat object to populate the version of the znode
   * @return data of the specified znode, or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte[] getDataAndWatch(ZooKeeperWatcher zkw, String znode,
      Stat stat) throws KeeperException {
    return getDataInternal(zkw, znode, stat, true);
  }

  private static byte[] getDataInternal(ZooKeeperWatcher zkw, String znode, Stat stat,
      boolean watcherSet)
      throws KeeperException {
    try {
      byte [] data = zkw.getRecoverableZooKeeper().getData(znode, zkw, stat);
      logRetrievedMsg(zkw, znode, data, watcherSet);
      return data;
    } catch (KeeperException.NoNodeException e) {
      // This log can get pretty annoying when we cycle on 100ms waits.
      // Enable trace if you really want to see it.
      LOG.trace(zkw.prefix("Unable to get data of znode " + znode + " " +
        "because node does not exist (not an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Get the data at the specified znode without setting a watch.
   *
   * Returns the data if the node exists.  Returns null if the node does not
   * exist.
   *
   * Sets the stats of the node in the passed Stat object.  Pass a null stat if
   * not interested.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param stat node status to get if node exists
   * @return data of the specified znode, or null if node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static byte [] getDataNoWatch(ZooKeeperWatcher zkw, String znode,
      Stat stat)
  throws KeeperException {
    try {
      byte [] data = zkw.getRecoverableZooKeeper().getData(znode, null, stat);
      logRetrievedMsg(zkw, znode, data, false);
      return data;
    } catch (KeeperException.NoNodeException e) {
      LOG.debug(zkw.prefix("Unable to get data of znode " + znode + " " +
          "because node does not exist (not necessarily an error)"));
      return null;
    } catch (KeeperException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.keeperException(e);
      return null;
    } catch (InterruptedException e) {
      LOG.warn(zkw.prefix("Unable to get data of znode " + znode), e);
      zkw.interruptedException(e);
      return null;
    }
  }

  /**
   * Returns the date of child znodes of the specified znode.  Also sets a watch on
   * the specified znode which will capture a NodeDeleted event on the specified
   * znode as well as NodeChildrenChanged if any children of the specified znode
   * are created or deleted.
   *
   * Returns null if the specified node does not exist.  Otherwise returns a
   * list of children of the specified node.  If the node exists but it has no
   * children, an empty list will be returned.
   *
   * @param zkw zk reference
   * @param baseNode path of node to list and watch children of
   * @return list of data of children of the specified node, an empty list if the node
   *          exists but has no children, and null if the node does not exist
   * @throws KeeperException if unexpected zookeeper exception
   * @deprecated Unused
   */
  @Deprecated
  public static List<NodeAndData> getChildDataAndWatchForNewChildren(
      ZooKeeperWatcher zkw, String baseNode) throws KeeperException {
    List<String> nodes =
      ZKUtil.listChildrenAndWatchForNewChildren(zkw, baseNode);
    if (nodes != null) {
      List<NodeAndData> newNodes = new ArrayList<NodeAndData>();
      for (String node : nodes) {
        String nodePath = ZKUtil.joinZNode(baseNode, node);
        byte[] data = ZKUtil.getDataAndWatch(zkw, nodePath);
        newNodes.add(new NodeAndData(nodePath, data));
      }
      return newNodes;
    }
    return null;
  }

  /**
   * Update the data of an existing node with the expected version to have the
   * specified data.
   *
   * Throws an exception if there is a version mismatch or some other problem.
   *
   * Sets no watches under any conditions.
   *
   * @param zkw zk reference
   * @param znode
   * @param data
   * @param expectedVersion
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.BadVersionException if version mismatch
   * @deprecated Unused
   */
  @Deprecated
  public static void updateExistingNodeData(ZooKeeperWatcher zkw, String znode,
      byte [] data, int expectedVersion)
  throws KeeperException {
    try {
      zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Data setting
  //

  /**
   * Sets the data of the existing znode to be the specified data.  Ensures that
   * the current data has the specified expected version.
   *
   * <p>If the node does not exist, a {@link NoNodeException} will be thrown.
   *
   * <p>If their is a version mismatch, method returns null.
   *
   * <p>No watches are set but setting data will trigger other watchers of this
   * node.
   *
   * <p>If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @param expectedVersion version expected when setting data
   * @return true if data set, false if version mismatch
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean setData(ZooKeeperWatcher zkw, String znode,
      byte [] data, int expectedVersion)
  throws KeeperException, KeeperException.NoNodeException {
    try {
      return zkw.getRecoverableZooKeeper().setData(znode, data, expectedVersion) != null;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
  }

  /**
   * Set data into node creating node if it doesn't yet exist.
   * Does not set watch.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @throws KeeperException
   */
  public static void createSetData(final ZooKeeperWatcher zkw, final String znode,
      final byte [] data)
  throws KeeperException {
    if (checkExists(zkw, znode) == -1) {
      ZKUtil.createWithParents(zkw, znode, data);
    } else {
      ZKUtil.setData(zkw, znode, data);
    }
  }

  /**
   * Sets the data of the existing znode to be the specified data.  The node
   * must exist but no checks are done on the existing data or version.
   *
   * <p>If the node does not exist, a {@link NoNodeException} will be thrown.
   *
   * <p>No watches are set but setting data will trigger other watchers of this
   * node.
   *
   * <p>If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data to set for node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void setData(ZooKeeperWatcher zkw, String znode, byte [] data)
  throws KeeperException, KeeperException.NoNodeException {
    setData(zkw, (SetData)ZKUtilOp.setData(znode, data));
  }

  private static void setData(ZooKeeperWatcher zkw, SetData setData)
  throws KeeperException, KeeperException.NoNodeException {
    SetDataRequest sd = (SetDataRequest)toZooKeeperOp(zkw, setData).toRequestRecord();
    setData(zkw, sd.getPath(), sd.getData(), sd.getVersion());
  }

  /**
   * Returns whether or not secure authentication is enabled
   * (whether <code>hbase.security.authentication</code> is set to
   * <code>kerberos</code>.
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

  private static ArrayList<ACL> createACL(ZooKeeperWatcher zkw, String node) {
    return createACL(zkw, node, isSecureZooKeeper(zkw.getConfiguration()));
  }

  public static ArrayList<ACL> createACL(ZooKeeperWatcher zkw, String node,
    boolean isSecureZooKeeper) {
    if (!node.startsWith(zkw.baseZNode)) {
      return Ids.OPEN_ACL_UNSAFE;
    }
    if (isSecureZooKeeper) {
      ArrayList<ACL> acls = new ArrayList<ACL>();
      // add permission to hbase supper user
      String[] superUsers = zkw.getConfiguration().getStrings(Superusers.SUPERUSER_CONF_KEY);
      if (superUsers != null) {
        List<String> groups = new ArrayList<String>();
        for (String user : superUsers) {
          if (AuthUtil.isGroupPrincipal(user)) {
            // TODO: Set node ACL for groups when ZK supports this feature
            groups.add(user);
          } else {
            acls.add(new ACL(Perms.ALL, new Id("auth", user)));
          }
        }
        if (!groups.isEmpty()) {
          LOG.warn("Znode ACL setting for group " + groups
              + " is skipped, ZooKeeper doesn't support this feature presently.");
        }
      }
      // Certain znodes are accessed directly by the client,
      // so they must be readable by non-authenticated clients
      if (zkw.isClientReadable(node)) {
        acls.addAll(Ids.CREATOR_ALL_ACL);
        acls.addAll(Ids.READ_ACL_UNSAFE);
      } else {
        acls.addAll(Ids.CREATOR_ALL_ACL);
      }
      return acls;
    } else {
      return Ids.OPEN_ACL_UNSAFE;
    }
  }

  //
  // Node creation
  //

  /**
   *
   * Set the specified znode to be an ephemeral node carrying the specified
   * data.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createEphemeralNodeAndWatch(ZooKeeperWatcher zkw,
      String znode, byte [] data)
  throws KeeperException {
    boolean ret = true;
    try {
      zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode),
          CreateMode.EPHEMERAL);
    } catch (KeeperException.NodeExistsException nee) {
      ret = false;
    } catch (InterruptedException e) {
      LOG.info("Interrupted", e);
      Thread.currentThread().interrupt();
    }
    if(!watchAndCheckExists(zkw, znode)) {
      // It did exist but now it doesn't, try again
      return createEphemeralNodeAndWatch(zkw, znode, data);
    }
    return ret;
  }

  /**
   * Creates the specified znode to be a persistent node carrying the specified
   * data.
   *
   * Returns true if the node was successfully created, false if the node
   * already existed.
   *
   * If the node is created successfully, a watcher is also set on the node.
   *
   * If the node is not created successfully because it already exists, this
   * method will also set a watcher on the node but return false.
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @return true if node created, false if not, watch set in both cases
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static boolean createNodeIfNotExistsAndWatch(
      ZooKeeperWatcher zkw, String znode, byte [] data)
  throws KeeperException {
    boolean ret = true;
    try {
      zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode),
          CreateMode.PERSISTENT);
    } catch (KeeperException.NodeExistsException nee) {
      ret = false;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
    try {
      zkw.getRecoverableZooKeeper().exists(znode, zkw);
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return false;
    }
    return ret;
  }

  /**
   * Creates the specified znode with the specified data but does not watch it.
   *
   * Returns the znode of the newly created node
   *
   * If there is another problem, a KeeperException will be thrown.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data data of node
   * @param createMode specifying whether the node to be created is ephemeral and/or sequential
   * @return true name of the newly created znode or null
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static String createNodeIfNotExistsNoWatch(ZooKeeperWatcher zkw, String znode,
      byte[] data, CreateMode createMode) throws KeeperException {

    String createdZNode = null;
    try {
      createdZNode = zkw.getRecoverableZooKeeper().create(znode, data,
          createACL(zkw, znode), createMode);
    } catch (KeeperException.NodeExistsException nee) {
      return znode;
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return null;
    }
    return createdZNode;
  }

  /**
   * Creates the specified node with the specified data and watches it.
   *
   * <p>Throws an exception if the node already exists.
   *
   * <p>The node created is persistent and open access.
   *
   * <p>Returns the version number of the created node if successful.
   *
   * @param zkw zk reference
   * @param znode path of node to create
   * @param data data of node to create
   * @return version of node created
   * @throws KeeperException if unexpected zookeeper exception
   * @throws KeeperException.NodeExistsException if node already exists
   */
  public static int createAndWatch(ZooKeeperWatcher zkw,
      String znode, byte [] data)
  throws KeeperException, KeeperException.NodeExistsException {
    try {
      zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode),
          CreateMode.PERSISTENT);
      Stat stat = zkw.getRecoverableZooKeeper().exists(znode, zkw);
      if (stat == null){
        // Likely a race condition. Someone deleted the znode.
        throw KeeperException.create(KeeperException.Code.SYSTEMERROR,
            "ZK.exists returned null (i.e.: znode does not exist) for znode=" + znode);
      }
     return stat.getVersion();
    } catch (InterruptedException e) {
      zkw.interruptedException(e);
      return -1;
    }
  }

  /**
   * Async creates the specified node with the specified data.
   *
   * <p>Throws an exception if the node already exists.
   *
   * <p>The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node to create
   * @param data data of node to create
   * @param cb
   * @param ctx
   */
  public static void asyncCreate(ZooKeeperWatcher zkw,
      String znode, byte [] data, final AsyncCallback.StringCallback cb,
      final Object ctx) {
    zkw.getRecoverableZooKeeper().getZooKeeper().create(znode, data,
        createACL(zkw, znode), CreateMode.PERSISTENT, cb, ctx);
  }

  /**
   * Creates the specified node, iff the node does not exist.  Does not set a
   * watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createAndFailSilent(ZooKeeperWatcher zkw,
      String znode) throws KeeperException {
    createAndFailSilent(zkw, znode, new byte[0]);
  }

  /**
   * Creates the specified node containing specified data, iff the node does not exist.  Does
   * not set a watch and fails silently if the node already exists.
   *
   * The node created is persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @param data a byte array data to store in the znode
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createAndFailSilent(ZooKeeperWatcher zkw,
      String znode, byte[] data)
  throws KeeperException {
    createAndFailSilent(zkw,
        (CreateAndFailSilent)ZKUtilOp.createAndFailSilent(znode, data));
  }

  private static void createAndFailSilent(ZooKeeperWatcher zkw, CreateAndFailSilent cafs)
  throws KeeperException {
    CreateRequest create = (CreateRequest)toZooKeeperOp(zkw, cafs).toRequestRecord();
    String znode = create.getPath();
    try {
      RecoverableZooKeeper zk = zkw.getRecoverableZooKeeper();
      if (zk.exists(znode, false) == null) {
        zk.create(znode, create.getData(), create.getAcl(), CreateMode.fromFlag(create.getFlags()));
      }
    } catch(KeeperException.NodeExistsException nee) {
    } catch(KeeperException.NoAuthException nee){
      try {
        if (null == zkw.getRecoverableZooKeeper().exists(znode, false)) {
          // If we failed to create the file and it does not already exist.
          throw(nee);
        }
      } catch (InterruptedException ie) {
        zkw.interruptedException(ie);
      }
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  /**
   * Creates the specified node and all parent nodes required for it to exist.
   *
   * No watches are set and no errors are thrown if the node already exists.
   *
   * The nodes created are persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createWithParents(ZooKeeperWatcher zkw, String znode)
  throws KeeperException {
    createWithParents(zkw, znode, new byte[0]);
  }

  /**
   * Creates the specified node and all parent nodes required for it to exist.  The creation of
   * parent znodes is not atomic with the leafe znode creation but the data is written atomically
   * when the leaf node is created.
   *
   * No watches are set and no errors are thrown if the node already exists.
   *
   * The nodes created are persistent and open access.
   *
   * @param zkw zk reference
   * @param znode path of node
   * @throws KeeperException if unexpected zookeeper exception
   */
  public static void createWithParents(ZooKeeperWatcher zkw, String znode, byte[] data)
  throws KeeperException {
    try {
      if(znode == null) {
        return;
      }
      zkw.getRecoverableZooKeeper().create(znode, data, createACL(zkw, znode),
          CreateMode.PERSISTENT);
    } catch(KeeperException.NodeExistsException nee) {
      return;
    } catch(KeeperException.NoNodeException nne) {
      createWithParents(zkw, getParent(znode));
      createWithParents(zkw, znode, data);
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }

  //
  // Deletes
  //

  /**
   * Delete the specified node.  Sets no watches.  Throws all exceptions.
   */
  public static void deleteNode(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    deleteNode(zkw, node, -1);
  }

  /**
   * Delete the specified node with the specified version.  Sets no watches.
   * Throws all exceptions.
   */
  public static boolean deleteNode(ZooKeeperWatcher zkw, String node,
      int version)
  throws KeeperException {
    try {
      zkw.getRecoverableZooKeeper().delete(node, version);
      return true;
    } catch(KeeperException.BadVersionException bve) {
      return false;
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
      return false;
    }
  }

  /**
   * Deletes the specified node.  Fails silent if the node does not exist.
   * @param zkw
   * @param node
   * @throws KeeperException
   */
  public static void deleteNodeFailSilent(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    deleteNodeFailSilent(zkw,
      (DeleteNodeFailSilent)ZKUtilOp.deleteNodeFailSilent(node));
  }

  private static void deleteNodeFailSilent(ZooKeeperWatcher zkw,
      DeleteNodeFailSilent dnfs) throws KeeperException {
    DeleteRequest delete = (DeleteRequest)toZooKeeperOp(zkw, dnfs).toRequestRecord();
    try {
      zkw.getRecoverableZooKeeper().delete(delete.getPath(), delete.getVersion());
    } catch(KeeperException.NoNodeException nne) {
    } catch(InterruptedException ie) {
      zkw.interruptedException(ie);
    }
  }


  /**
   * Delete the specified node and all of it's children.
   * <p>
   * If the node does not exist, just returns.
   * <p>
   * Sets no watches. Throws all exceptions besides dealing with deletion of
   * children.
   */
  public static void deleteNodeRecursively(ZooKeeperWatcher zkw, String node)
  throws KeeperException {
    deleteNodeRecursivelyMultiOrSequential(zkw, true, node);
  }

  /**
   * Delete all the children of the specified node but not the node itself.
   *
   * Sets no watches.  Throws all exceptions besides dealing with deletion of
   * children.
   *
   * If hbase.zookeeper.useMulti is true, use ZooKeeper's multi-update functionality.
   * Otherwise, run the list of operations sequentially.
   *
   * @throws KeeperException
   */
  public static void deleteChildrenRecursively(ZooKeeperWatcher zkw, String node)
      throws KeeperException {
    deleteChildrenRecursivelyMultiOrSequential(zkw, true, node);
  }

  /**
   * Delete all the children of the specified node but not the node itself. This
   * will first traverse the znode tree for listing the children and then delete
   * these znodes using multi-update api or sequential based on the specified
   * configurations.
   * <p>
   * Sets no watches. Throws all exceptions besides dealing with deletion of
   * children.
   * <p>
   * If hbase.zookeeper.useMulti is true, use ZooKeeper's multi-update
   * functionality. Otherwise, run the list of operations sequentially.
   * <p>
   * If all of the following are true:
   * <ul>
   * <li>runSequentialOnMultiFailure is true
   * <li>hbase.zookeeper.useMulti is true
   * </ul>
   * on calling multi, we get a ZooKeeper exception that can be handled by a
   * sequential call(*), we retry the operations one-by-one (sequentially).
   *
   * @param zkw
   *          - zk reference
   * @param runSequentialOnMultiFailure
   *          - if true when we get a ZooKeeper exception that could retry the
   *          operations one-by-one (sequentially)
   * @param pathRoots
   *          - path of the parent node(s)
   * @throws KeeperException.NotEmptyException
   *           if node has children while deleting
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   * @throws IllegalArgumentException
   *           if an invalid path is specified
   */
  public static void deleteChildrenRecursivelyMultiOrSequential(
      ZooKeeperWatcher zkw, boolean runSequentialOnMultiFailure,
      String... pathRoots) throws KeeperException {
    if (pathRoots == null || pathRoots.length <= 0) {
      LOG.warn("Given path is not valid!");
      return;
    }
    List<ZKUtilOp> ops = new ArrayList<ZKUtil.ZKUtilOp>();
    for (String eachRoot : pathRoots) {
      List<String> children = listChildrenBFSNoWatch(zkw, eachRoot);
      // Delete the leaves first and eventually get rid of the root
      for (int i = children.size() - 1; i >= 0; --i) {
        ops.add(ZKUtilOp.deleteNodeFailSilent(children.get(i)));
      }
    }
    // atleast one element should exist
    if (ops.size() > 0) {
      multiOrSequential(zkw, ops, runSequentialOnMultiFailure);
    }
  }

  /**
   * Delete the specified node and its children. This traverse the
   * znode tree for listing the children and then delete
   * these znodes including the parent using multi-update api or
   * sequential based on the specified configurations.
   * <p>
   * Sets no watches. Throws all exceptions besides dealing with deletion of
   * children.
   * <p>
   * If hbase.zookeeper.useMulti is true, use ZooKeeper's multi-update
   * functionality. Otherwise, run the list of operations sequentially.
   * <p>
   * If all of the following are true:
   * <ul>
   * <li>runSequentialOnMultiFailure is true
   * <li>hbase.zookeeper.useMulti is true
   * </ul>
   * on calling multi, we get a ZooKeeper exception that can be handled by a
   * sequential call(*), we retry the operations one-by-one (sequentially).
   *
   * @param zkw
   *          - zk reference
   * @param runSequentialOnMultiFailure
   *          - if true when we get a ZooKeeper exception that could retry the
   *          operations one-by-one (sequentially)
   * @param pathRoots
   *          - path of the parent node(s)
   * @throws KeeperException.NotEmptyException
   *           if node has children while deleting
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   * @throws IllegalArgumentException
   *           if an invalid path is specified
   */
  public static void deleteNodeRecursivelyMultiOrSequential(ZooKeeperWatcher zkw,
      boolean runSequentialOnMultiFailure, String... pathRoots) throws KeeperException {
    if (pathRoots == null || pathRoots.length <= 0) {
      LOG.warn("Given path is not valid!");
      return;
    }
    List<ZKUtilOp> ops = new ArrayList<ZKUtil.ZKUtilOp>();
    for (String eachRoot : pathRoots) {
      // ZooKeeper Watches are one time triggers; When children of parent nodes are deleted
      // recursively, must set another watch, get notified of delete node
      List<String> children = listChildrenBFSAndWatchThem(zkw, eachRoot);
      // Delete the leaves first and eventually get rid of the root
      for (int i = children.size() - 1; i >= 0; --i) {
        ops.add(ZKUtilOp.deleteNodeFailSilent(children.get(i)));
      }
      try {
        if (zkw.getRecoverableZooKeeper().exists(eachRoot, zkw) != null) {
          ops.add(ZKUtilOp.deleteNodeFailSilent(eachRoot));
        }
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
      }
    }
    // atleast one element should exist
    if (ops.size() > 0) {
      multiOrSequential(zkw, ops, runSequentialOnMultiFailure);
    }
  }

  /**
   * BFS Traversal of all the children under path, with the entries in the list,
   * in the same order as that of the traversal. Lists all the children without
   * setting any watches.
   *
   * @param zkw
   *          - zk reference
   * @param znode
   *          - path of node
   * @return list of children znodes under the path
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   */
  private static List<String> listChildrenBFSNoWatch(ZooKeeperWatcher zkw,
      final String znode) throws KeeperException {
    Deque<String> queue = new LinkedList<String>();
    List<String> tree = new ArrayList<String>();
    queue.add(znode);
    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }
      List<String> children = listChildrenNoWatch(zkw, node);
      if (children == null) {
        continue;
      }
      for (final String child : children) {
        final String childPath = node + "/" + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }

  /**
   * BFS Traversal of all the children under path, with the entries in the list,
   * in the same order as that of the traversal.
   * Lists all the children and set watches on to them.
   *
   * @param zkw
   *          - zk reference
   * @param znode
   *          - path of node
   * @return list of children znodes under the path
   * @throws KeeperException
   *           if unexpected ZooKeeper exception
   */
  private static List<String> listChildrenBFSAndWatchThem(ZooKeeperWatcher zkw, final String znode)
      throws KeeperException {
    Deque<String> queue = new LinkedList<String>();
    List<String> tree = new ArrayList<String>();
    queue.add(znode);
    while (true) {
      String node = queue.pollFirst();
      if (node == null) {
        break;
      }
      List<String> children = listChildrenAndWatchThem(zkw, node);
      if (children == null) {
        continue;
      }
      for (final String child : children) {
        final String childPath = node + "/" + child;
        queue.add(childPath);
        tree.add(childPath);
      }
    }
    return tree;
  }

  /**
   * Represents an action taken by ZKUtil, e.g. createAndFailSilent.
   * These actions are higher-level than ZKOp actions, which represent
   * individual actions in the ZooKeeper API, like create.
   */
  public abstract static class ZKUtilOp {
    private String path;

    private ZKUtilOp(String path) {
      this.path = path;
    }

    /**
     * @return a createAndFailSilent ZKUtilOp
     */
    public static ZKUtilOp createAndFailSilent(String path, byte[] data) {
      return new CreateAndFailSilent(path, data);
    }

    /**
     * @return a deleteNodeFailSilent ZKUtilOP
     */
    public static ZKUtilOp deleteNodeFailSilent(String path) {
      return new DeleteNodeFailSilent(path);
    }

    /**
     * @return a setData ZKUtilOp
     */
    public static ZKUtilOp setData(String path, byte [] data) {
      return new SetData(path, data);
    }

    /**
     * @return path to znode where the ZKOp will occur
     */
    public String getPath() {
      return path;
    }

    /**
     * ZKUtilOp representing createAndFailSilent in ZooKeeper
     * (attempt to create node, ignore error if already exists)
     */
    public static class CreateAndFailSilent extends ZKUtilOp {
      private byte [] data;

      private CreateAndFailSilent(String path, byte [] data) {
        super(path);
        this.data = data;
      }

      public byte[] getData() {
        return data;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof CreateAndFailSilent)) return false;

        CreateAndFailSilent op = (CreateAndFailSilent) o;
        return getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
      }

      @Override
      public int hashCode() {
        int ret = 17 + getPath().hashCode() * 31;
        return ret * 31 + Bytes.hashCode(data);
      }
    }

    /**
     * ZKUtilOp representing deleteNodeFailSilent in ZooKeeper
     * (attempt to delete node, ignore error if node doesn't exist)
     */
    public static class DeleteNodeFailSilent extends ZKUtilOp {
      private DeleteNodeFailSilent(String path) {
        super(path);
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DeleteNodeFailSilent)) return false;

        return super.equals(o);
      }

      @Override
      public int hashCode() {
        return getPath().hashCode();
      }
    }

    /**
     * ZKUtilOp representing setData in ZooKeeper
     */
    public static class SetData extends ZKUtilOp {
      private byte [] data;

      private SetData(String path, byte [] data) {
        super(path);
        this.data = data;
      }

      public byte[] getData() {
        return data;
      }

      @Override
      public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SetData)) return false;

        SetData op = (SetData) o;
        return getPath().equals(op.getPath()) && Arrays.equals(data, op.data);
      }

      @Override
      public int hashCode() {
        int ret = getPath().hashCode();
        return ret * 31 + Bytes.hashCode(data);
      }
    }
  }

  /**
   * Convert from ZKUtilOp to ZKOp
   */
  private static Op toZooKeeperOp(ZooKeeperWatcher zkw, ZKUtilOp op)
  throws UnsupportedOperationException {
    if(op == null) return null;

    if (op instanceof CreateAndFailSilent) {
      CreateAndFailSilent cafs = (CreateAndFailSilent)op;
      return Op.create(cafs.getPath(), cafs.getData(), createACL(zkw, cafs.getPath()),
        CreateMode.PERSISTENT);
    } else if (op instanceof DeleteNodeFailSilent) {
      DeleteNodeFailSilent dnfs = (DeleteNodeFailSilent)op;
      return Op.delete(dnfs.getPath(), -1);
    } else if (op instanceof SetData) {
      SetData sd = (SetData)op;
      return Op.setData(sd.getPath(), sd.getData(), -1);
    } else {
      throw new UnsupportedOperationException("Unexpected ZKUtilOp type: "
        + op.getClass().getName());
    }
  }

  /**
   * If hbase.zookeeper.useMulti is true, use ZooKeeper's multi-update functionality.
   * Otherwise, run the list of operations sequentially.
   *
   * If all of the following are true:
   * - runSequentialOnMultiFailure is true
   * - hbase.zookeeper.useMulti is true
   * - on calling multi, we get a ZooKeeper exception that can be handled by a sequential call(*)
   * Then:
   * - we retry the operations one-by-one (sequentially)
   *
   * Note *: an example is receiving a NodeExistsException from a "create" call.  Without multi,
   * a user could call "createAndFailSilent" to ensure that a node exists if they don't care who
   * actually created the node (i.e. the NodeExistsException from ZooKeeper is caught).
   * This will cause all operations in the multi to fail, however, because
   * the NodeExistsException that zk.create throws will fail the multi transaction.
   * In this case, if the previous conditions hold, the commands are run sequentially, which should
   * result in the correct final state, but means that the operations will not run atomically.
   *
   * @throws KeeperException
   */
  public static void multiOrSequential(ZooKeeperWatcher zkw, List<ZKUtilOp> ops,
      boolean runSequentialOnMultiFailure) throws KeeperException {
    if (ops == null) return;
    boolean useMulti = zkw.getConfiguration().getBoolean(HConstants.ZOOKEEPER_USEMULTI, false);

    if (useMulti) {
      List<Op> zkOps = new LinkedList<Op>();
      for (ZKUtilOp op : ops) {
        zkOps.add(toZooKeeperOp(zkw, op));
      }
      try {
        zkw.getRecoverableZooKeeper().multi(zkOps);
      } catch (KeeperException ke) {
       switch (ke.code()) {
         case NODEEXISTS:
         case NONODE:
         case BADVERSION:
         case NOAUTH:
           // if we get an exception that could be solved by running sequentially
           // (and the client asked us to), then break out and run sequentially
           if (runSequentialOnMultiFailure) {
             LOG.info("On call to ZK.multi, received exception: " + ke.toString() + "."
               + "  Attempting to run operations sequentially because"
               + " runSequentialOnMultiFailure is: " + runSequentialOnMultiFailure + ".");
             processSequentially(zkw, ops);
             break;
           }
          default:
            throw ke;
        }
      } catch (InterruptedException ie) {
        zkw.interruptedException(ie);
      }
    } else {
      // run sequentially
      processSequentially(zkw, ops);
    }

  }

  private static void processSequentially(ZooKeeperWatcher zkw, List<ZKUtilOp> ops)
      throws KeeperException, NoNodeException {
    for (ZKUtilOp op : ops) {
      if (op instanceof CreateAndFailSilent) {
        createAndFailSilent(zkw, (CreateAndFailSilent) op);
      } else if (op instanceof DeleteNodeFailSilent) {
        deleteNodeFailSilent(zkw, (DeleteNodeFailSilent) op);
      } else if (op instanceof SetData) {
        setData(zkw, (SetData) op);
      } else {
        throw new UnsupportedOperationException("Unexpected ZKUtilOp type: "
            + op.getClass().getName());
      }
    }
  }

  //
  // ZooKeeper cluster information
  //

  /** @return String dump of everything in ZooKeeper. */
  public static String dump(ZooKeeperWatcher zkw) {
    StringBuilder sb = new StringBuilder();
    try {
      sb.append("HBase is rooted at ").append(zkw.baseZNode);
      sb.append("\nActive master address: ");
      try {
        sb.append(MasterAddressTracker.getMasterAddress(zkw));
      } catch (IOException e) {
        sb.append("<<FAILED LOOKUP: " + e.getMessage() + ">>");
      }
      sb.append("\nBackup master addresses:");
      for (String child : listChildrenNoWatch(zkw,
                                              zkw.backupMasterAddressesZNode)) {
        sb.append("\n ").append(child);
      }
      sb.append("\nRegion server holding hbase:meta: "
        + new MetaTableLocator().getMetaRegionLocation(zkw));
      Configuration conf = HBaseConfiguration.create();
      int numMetaReplicas = conf.getInt(HConstants.META_REPLICAS_NUM,
               HConstants.DEFAULT_META_REPLICA_NUM);
      for (int i = 1; i < numMetaReplicas; i++) {
        sb.append("\nRegion server holding hbase:meta, replicaId " + i + " "
                    + new MetaTableLocator().getMetaRegionLocation(zkw, i));
      }
      sb.append("\nRegion servers:");
      for (String child : listChildrenNoWatch(zkw, zkw.rsZNode)) {
        sb.append("\n ").append(child);
      }
      try {
        getReplicationZnodesDump(zkw, sb);
      } catch (KeeperException ke) {
        LOG.warn("Couldn't get the replication znode dump", ke);
      }
      sb.append("\nQuorum Server Statistics:");
      String[] servers = zkw.getQuorum().split(",");
      for (String server : servers) {
        sb.append("\n ").append(server);
        try {
          String[] stat = getServerStats(server, ZKUtil.zkDumpConnectionTimeOut);

          if (stat == null) {
            sb.append("[Error] invalid quorum server: " + server);
            break;
          }

          for (String s : stat) {
            sb.append("\n  ").append(s);
          }
        } catch (Exception e) {
          sb.append("\n  ERROR: ").append(e.getMessage());
        }
      }
    } catch (KeeperException ke) {
      sb.append("\nFATAL ZooKeeper Exception!\n");
      sb.append("\n" + ke.getMessage());
    }
    return sb.toString();
  }

  /**
   * Appends replication znodes to the passed StringBuilder.
   * @param zkw
   * @param sb
   * @throws KeeperException
   */
  private static void getReplicationZnodesDump(ZooKeeperWatcher zkw, StringBuilder sb)
      throws KeeperException {
    String replicationZNodeName = zkw.getConfiguration().get("zookeeper.znode.replication",
      "replication");
    String replicationZnode = joinZNode(zkw.baseZNode, replicationZNodeName);
    if (ZKUtil.checkExists(zkw, replicationZnode) == -1) return;
    // do a ls -r on this znode
    sb.append("\n").append(replicationZnode).append(": ");
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, replicationZnode);
    for (String child : children) {
      String znode = joinZNode(replicationZnode, child);
      if (child.equals(zkw.getConfiguration().get("zookeeper.znode.replication.peers", "peers"))) {
        appendPeersZnodes(zkw, znode, sb);
      } else if (child.equals(zkw.getConfiguration().
          get("zookeeper.znode.replication.rs", "rs"))) {
        appendRSZnodes(zkw, znode, sb);
      } else if (child.equals(zkw.getConfiguration().get(
        ReplicationStateZKBase.ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_KEY,
        ReplicationStateZKBase.ZOOKEEPER_ZNODE_REPLICATION_HFILE_REFS_DEFAULT))) {
        appendHFileRefsZnodes(zkw, znode, sb);
      }
    }
  }

  private static void appendHFileRefsZnodes(ZooKeeperWatcher zkw, String hfileRefsZnode,
      StringBuilder sb) throws KeeperException {
    sb.append("\n").append(hfileRefsZnode).append(": ");
    for (String peerIdZnode : ZKUtil.listChildrenNoWatch(zkw, hfileRefsZnode)) {
      String znodeToProcess = ZKUtil.joinZNode(hfileRefsZnode, peerIdZnode);
      sb.append("\n").append(znodeToProcess).append(": ");
      List<String> peerHFileRefsZnodes = ZKUtil.listChildrenNoWatch(zkw, znodeToProcess);
      int size = peerHFileRefsZnodes.size();
      for (int i = 0; i < size; i++) {
        sb.append(peerHFileRefsZnodes.get(i));
        if (i != size - 1) {
          sb.append(", ");
        }
      }
    }
  }

  private static void appendRSZnodes(ZooKeeperWatcher zkw, String znode, StringBuilder sb)
      throws KeeperException {
    List<String> stack = new LinkedList<String>();
    stack.add(znode);
    do {
      String znodeToProcess = stack.remove(stack.size() - 1);
      sb.append("\n").append(znodeToProcess).append(": ");
      byte[] data;
      try {
        data = ZKUtil.getData(zkw, znodeToProcess);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return;
      }
      if (data != null && data.length > 0) { // log position
        long position = 0;
        try {
          position = ZKUtil.parseWALPositionFrom(ZKUtil.getData(zkw, znodeToProcess));
          sb.append(position);
        } catch (DeserializationException ignored) {
        } catch (InterruptedException e) {
          zkw.interruptedException(e);
          return;
        }
      }
      for (String zNodeChild : ZKUtil.listChildrenNoWatch(zkw, znodeToProcess)) {
        stack.add(ZKUtil.joinZNode(znodeToProcess, zNodeChild));
      }
    } while (stack.size() > 0);
  }

  private static void appendPeersZnodes(ZooKeeperWatcher zkw, String peersZnode,
    StringBuilder sb) throws KeeperException {
    int pblen = ProtobufUtil.lengthOfPBMagic();
    sb.append("\n").append(peersZnode).append(": ");
    for (String peerIdZnode : ZKUtil.listChildrenNoWatch(zkw, peersZnode)) {
      String znodeToProcess = ZKUtil.joinZNode(peersZnode, peerIdZnode);
      byte[] data;
      try {
        data = ZKUtil.getData(zkw, znodeToProcess);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return;
      }
      // parse the data of the above peer znode.
      try {
        ZooKeeperProtos.ReplicationPeer.Builder builder =
          ZooKeeperProtos.ReplicationPeer.newBuilder();
        ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
        String clusterKey = builder.getClusterkey();
        sb.append("\n").append(znodeToProcess).append(": ").append(clusterKey);
        // add the peer-state.
        appendPeerState(zkw, znodeToProcess, sb);
      } catch (IOException ipbe) {
        LOG.warn("Got Exception while parsing peer: " + znodeToProcess, ipbe);
      }
    }
  }

  private static void appendPeerState(ZooKeeperWatcher zkw, String znodeToProcess,
      StringBuilder sb) throws KeeperException, InvalidProtocolBufferException {
    String peerState = zkw.getConfiguration().get("zookeeper.znode.replication.peers.state",
      "peer-state");
    int pblen = ProtobufUtil.lengthOfPBMagic();
    for (String child : ZKUtil.listChildrenNoWatch(zkw, znodeToProcess)) {
      if (!child.equals(peerState)) continue;
      String peerStateZnode = ZKUtil.joinZNode(znodeToProcess, child);
      sb.append("\n").append(peerStateZnode).append(": ");
      byte[] peerStateData;
      try {
        peerStateData = ZKUtil.getData(zkw, peerStateZnode);
        ZooKeeperProtos.ReplicationState.Builder builder =
            ZooKeeperProtos.ReplicationState.newBuilder();
        ProtobufUtil.mergeFrom(builder, peerStateData, pblen, peerStateData.length - pblen);
        sb.append(builder.getState().name());
      } catch (IOException ipbe) {
        LOG.warn("Got Exception while parsing peer: " + znodeToProcess, ipbe);
      } catch (InterruptedException e) {
        zkw.interruptedException(e);
        return;
      }
    }
  }

  /**
   * Gets the statistics from the given server.
   *
   * @param server  The server to get the statistics from.
   * @param timeout  The socket timeout to use.
   * @return The array of response strings.
   * @throws IOException When the socket communication fails.
   */
  public static String[] getServerStats(String server, int timeout)
  throws IOException {
    String[] sp = server.split(":");
    if (sp == null || sp.length == 0) {
      return null;
    }

    String host = sp[0];
    int port = sp.length > 1 ? Integer.parseInt(sp[1])
        : HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;

    Socket socket = new Socket();
    InetSocketAddress sockAddr = new InetSocketAddress(host, port);
    socket.connect(sockAddr, timeout);

    socket.setSoTimeout(timeout);
    PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
    BufferedReader in = new BufferedReader(new InputStreamReader(
      socket.getInputStream()));
    out.println("stat");
    out.flush();
    ArrayList<String> res = new ArrayList<String>();
    while (true) {
      String line = in.readLine();
      if (line != null) {
        res.add(line);
      } else {
        break;
      }
    }
    socket.close();
    return res.toArray(new String[res.size()]);
  }

  private static void logRetrievedMsg(final ZooKeeperWatcher zkw,
      final String znode, final byte [] data, final boolean watcherSet) {
    if (!LOG.isTraceEnabled()) return;
    LOG.trace(zkw.prefix("Retrieved " + ((data == null)? 0: data.length) +
      " byte(s) of data from znode " + znode +
      (watcherSet? " and set watcher; ": "; data=") +
      (data == null? "null": data.length == 0? "empty": (
          znode.startsWith(ZooKeeperWatcher.META_ZNODE_PREFIX)?
            getServerNameOrEmptyString(data):
          znode.startsWith(zkw.backupMasterAddressesZNode)?
            getServerNameOrEmptyString(data):
          StringUtils.abbreviate(Bytes.toStringBinary(data), 32)))));
  }

  private static String getServerNameOrEmptyString(final byte [] data) {
    try {
      return ServerName.parseFrom(data).toString();
    } catch (DeserializationException e) {
      return "";
    }
  }

  /**
   * Waits for HBase installation's base (parent) znode to become available.
   * @throws IOException on ZK errors
   */
  public static void waitForBaseZNode(Configuration conf) throws IOException {
    LOG.info("Waiting until the base znode is available");
    String parentZNode = conf.get(HConstants.ZOOKEEPER_ZNODE_PARENT,
        HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    ZooKeeper zk = new ZooKeeper(ZKConfig.getZKQuorumServersString(conf),
        conf.getInt(HConstants.ZK_SESSION_TIMEOUT,
        HConstants.DEFAULT_ZK_SESSION_TIMEOUT), EmptyWatcher.instance);

    final int maxTimeMs = 10000;
    final int maxNumAttempts = maxTimeMs / HConstants.SOCKET_RETRY_WAIT_MS;

    KeeperException keeperEx = null;
    try {
      try {
        for (int attempt = 0; attempt < maxNumAttempts; ++attempt) {
          try {
            if (zk.exists(parentZNode, false) != null) {
              LOG.info("Parent znode exists: " + parentZNode);
              keeperEx = null;
              break;
            }
          } catch (KeeperException e) {
            keeperEx = e;
          }
          Threads.sleepWithoutInterrupt(HConstants.SOCKET_RETRY_WAIT_MS);
        }
      } finally {
        zk.close();
      }
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
    }

    if (keeperEx != null) {
      throw new IOException(keeperEx);
    }
  }

  /**
   * Convert a {@link DeserializationException} to a more palatable {@link KeeperException}.
   * Used when can't let a {@link DeserializationException} out w/o changing public API.
   * @param e Exception to convert
   * @return Converted exception
   */
  public static KeeperException convert(final DeserializationException e) {
    KeeperException ke = new KeeperException.DataInconsistencyException();
    ke.initCause(e);
    return ke;
  }

  /**
   * Recursively print the current state of ZK (non-transactional)
   * @param root name of the root directory in zk to print
   */
  public static void logZKTree(ZooKeeperWatcher zkw, String root) {
    if (!LOG.isDebugEnabled()) return;
    LOG.debug("Current zk system:");
    String prefix = "|-";
    LOG.debug(prefix + root);
    try {
      logZKTree(zkw, root, prefix);
    } catch (KeeperException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Helper method to print the current state of the ZK tree.
   * @see #logZKTree(ZooKeeperWatcher, String)
   * @throws KeeperException if an unexpected exception occurs
   */
  protected static void logZKTree(ZooKeeperWatcher zkw, String root, String prefix)
      throws KeeperException {
    List<String> children = ZKUtil.listChildrenNoWatch(zkw, root);
    if (children == null) return;
    for (String child : children) {
      LOG.debug(prefix + child);
      String node = ZKUtil.joinZNode(root.equals("/") ? "" : root, child);
      logZKTree(zkw, node, prefix + "---");
    }
  }

  /**
   * @param position
   * @return Serialized protobuf of <code>position</code> with pb magic prefix prepended suitable
   *         for use as content of an wal position in a replication queue.
   */
  public static byte[] positionToByteArray(final long position) {
    byte[] bytes = ZooKeeperProtos.ReplicationHLogPosition.newBuilder().setPosition(position)
        .build().toByteArray();
    return ProtobufUtil.prependPBMagic(bytes);
  }

  /**
   * @param bytes - Content of a WAL position znode.
   * @return long - The current WAL position.
   * @throws DeserializationException
   */
  public static long parseWALPositionFrom(final byte[] bytes) throws DeserializationException {
    if (bytes == null) {
      throw new DeserializationException("Unable to parse null WAL position.");
    }
    if (ProtobufUtil.isPBMagicPrefix(bytes)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      ZooKeeperProtos.ReplicationHLogPosition.Builder builder =
          ZooKeeperProtos.ReplicationHLogPosition.newBuilder();
      ZooKeeperProtos.ReplicationHLogPosition position;
      try {
        ProtobufUtil.mergeFrom(builder, bytes, pblen, bytes.length - pblen);
        position = builder.build();
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return position.getPosition();
    } else {
      if (bytes.length > 0) {
        return Bytes.toLong(bytes);
      }
      return 0;
    }
  }

  /**
   * @param regionLastFlushedSequenceId the flushed sequence id of a region which is the min of its
   *          store max seq ids
   * @param storeSequenceIds column family to sequence Id map
   * @return Serialized protobuf of <code>RegionSequenceIds</code> with pb magic prefix prepended
   *         suitable for use to filter wal edits in distributedLogReplay mode
   */
  public static byte[] regionSequenceIdsToByteArray(final Long regionLastFlushedSequenceId,
      final Map<byte[], Long> storeSequenceIds) {
    ClusterStatusProtos.RegionStoreSequenceIds.Builder regionSequenceIdsBuilder =
        ClusterStatusProtos.RegionStoreSequenceIds.newBuilder();
    ClusterStatusProtos.StoreSequenceId.Builder storeSequenceIdBuilder =
        ClusterStatusProtos.StoreSequenceId.newBuilder();
    if (storeSequenceIds != null) {
      for (Map.Entry<byte[], Long> e : storeSequenceIds.entrySet()){
        byte[] columnFamilyName = e.getKey();
        Long curSeqId = e.getValue();
        storeSequenceIdBuilder.setFamilyName(ByteStringer.wrap(columnFamilyName));
        storeSequenceIdBuilder.setSequenceId(curSeqId);
        regionSequenceIdsBuilder.addStoreSequenceId(storeSequenceIdBuilder.build());
        storeSequenceIdBuilder.clear();
      }
    }
    regionSequenceIdsBuilder.setLastFlushedSequenceId(regionLastFlushedSequenceId);
    byte[] result = regionSequenceIdsBuilder.build().toByteArray();
    return ProtobufUtil.prependPBMagic(result);
  }

  /**
   * @param bytes Content of serialized data of RegionStoreSequenceIds
   * @return a RegionStoreSequenceIds object
   * @throws DeserializationException
   */
  public static RegionStoreSequenceIds parseRegionStoreSequenceIds(final byte[] bytes)
      throws DeserializationException {
    if (bytes == null || !ProtobufUtil.isPBMagicPrefix(bytes)) {
      throw new DeserializationException("Unable to parse RegionStoreSequenceIds.");
    }
    RegionStoreSequenceIds.Builder regionSequenceIdsBuilder =
        ClusterStatusProtos.RegionStoreSequenceIds.newBuilder();
    int pblen = ProtobufUtil.lengthOfPBMagic();
    RegionStoreSequenceIds storeIds = null;
    try {
      ProtobufUtil.mergeFrom(regionSequenceIdsBuilder, bytes, pblen, bytes.length - pblen);
      storeIds = regionSequenceIdsBuilder.build();
    } catch (IOException e) {
      throw new DeserializationException(e);
    }
    return storeIds;
  }
}
