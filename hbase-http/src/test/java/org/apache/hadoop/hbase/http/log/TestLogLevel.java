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
package org.apache.hadoop.hbase.http.log;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.SocketException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLException;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtil;
import org.apache.hadoop.hbase.http.HttpConfig;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.http.log.LogLevel.CLI;
import org.apache.hadoop.hbase.http.ssl.KeyStoreTestUtil;
import org.apache.hadoop.hbase.logging.Log4jUtils;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.hadoop.util.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test LogLevel.
 */
@Category({ MiscTests.class, SmallTests.class })
public class TestLogLevel {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestLogLevel.class);

  private static String keystoresDir;
  private static String sslConfDir;
  private static Configuration serverConf;
  private static Configuration clientConf;
  private static Configuration sslConf;
  private static final String logName = TestLogLevel.class.getName();
  private static final String protectedPrefix = "protected";
  private static final String protectedLogName = protectedPrefix + "." + logName;
  private static final org.apache.logging.log4j.Logger log =
    org.apache.logging.log4j.LogManager.getLogger(logName);
  private final static String PRINCIPAL = "loglevel.principal";
  private final static String KEYTAB = "loglevel.keytab";

  private static MiniKdc kdc;

  private static final String LOCALHOST = "localhost";
  private static final String clientPrincipal = "client/" + LOCALHOST;
  private static String HTTP_PRINCIPAL = "HTTP/" + LOCALHOST;
  private static HBaseCommonTestingUtil HTU;
  private static File keyTabFile;
  private static final Pattern EFFECTIVE_LEVEL = Pattern.compile("Effective level:\\s*(\\S+)");

  @FunctionalInterface
  private interface ThrowingRunnable {
    void run() throws Exception;
  }

  @FunctionalInterface
  private interface ThrowingConsumer {
    void accept(String url) throws Exception;
  }

  private enum Protocol {
    C_HTTP_S_HTTP(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTP),
    C_HTTP_S_HTTPS(LogLevel.PROTOCOL_HTTP, LogLevel.PROTOCOL_HTTPS),
    C_HTTPS_S_HTTP(LogLevel.PROTOCOL_HTTPS, LogLevel.PROTOCOL_HTTP),
    C_HTTPS_S_HTTPS(LogLevel.PROTOCOL_HTTPS, LogLevel.PROTOCOL_HTTPS);

    final String client;
    final String server;

    Protocol(String client, String server) {
      this.client = client;
      this.server = server;
    }
  }

  @BeforeClass
  public static void setUp() throws Exception {
    serverConf = new Configuration();
    serverConf.setStrings(LogLevel.READONLY_LOGGERS_CONF_KEY, protectedPrefix);
    HTU = new HBaseCommonTestingUtil(serverConf);

    File keystoreDir = new File(HTU.getDataTestDir("keystore").toString());
    keystoreDir.mkdirs();
    keyTabFile = new File(HTU.getDataTestDir("keytab").toString(), "keytabfile");
    keyTabFile.getParentFile().mkdirs();
    clientConf = new Configuration();

    setupSSL(keystoreDir);

    kdc = setupMiniKdc();
    // Create two principles: a client and an HTTP principal
    kdc.createPrincipal(keyTabFile, clientPrincipal, HTTP_PRINCIPAL);
  }

  /**
   * Sets up {@link MiniKdc} for testing security. Copied from HBaseTestingUtility#setupMiniKdc().
   */
  static private MiniKdc setupMiniKdc() throws Exception {
    Properties conf = MiniKdc.createConf();
    conf.put(MiniKdc.DEBUG, true);
    MiniKdc kdc = null;
    File dir = null;
    // There is time lag between selecting a port and trying to bind with it. It's possible that
    // another service captures the port in between which'll result in BindException.
    boolean bindException;
    int numTries = 0;
    do {
      try {
        bindException = false;
        dir = new File(HTU.getDataTestDir("kdc").toUri().getPath());
        kdc = new MiniKdc(conf, dir);
        kdc.start();
      } catch (BindException e) {
        FileUtils.deleteDirectory(dir); // clean directory
        numTries++;
        if (numTries == 3) {
          log.error("Failed setting up MiniKDC. Tried " + numTries + " times.");
          throw e;
        }
        log.error("BindException encountered when setting up MiniKdc. Trying again.");
        bindException = true;
      }
    } while (bindException);
    return kdc;
  }

  static private void setupSSL(File base) throws Exception {
    clientConf.set(DFSConfigKeys.DFS_HTTP_POLICY_KEY, HttpConfig.Policy.HTTPS_ONLY.name());
    clientConf.set(DFSConfigKeys.DFS_NAMENODE_HTTPS_ADDRESS_KEY, "localhost:0");
    clientConf.set(DFSConfigKeys.DFS_DATANODE_HTTPS_ADDRESS_KEY, "localhost:0");

    keystoresDir = base.getAbsolutePath();
    sslConfDir = KeyStoreTestUtil.getClasspathDir(TestLogLevel.class);
    KeyStoreTestUtil.setupSSLConfig(keystoresDir, sslConfDir, serverConf, false);

    sslConf = getSslConfig(serverConf);
  }

  /**
   * Get the SSL configuration. This method is copied from KeyStoreTestUtil#getSslConfig() in
   * Hadoop.
   * @return {@link Configuration} instance with ssl configs loaded.
   * @param conf to pull client/server SSL settings filename from
   */
  private static Configuration getSslConfig(Configuration conf) {
    Configuration sslConf = new Configuration(false);
    String sslServerConfFile = conf.get(SSLFactory.SSL_SERVER_CONF_KEY);
    String sslClientConfFile = conf.get(SSLFactory.SSL_CLIENT_CONF_KEY);
    sslConf.addResource(sslServerConfFile);
    sslConf.addResource(sslClientConfFile);
    sslConf.set(SSLFactory.SSL_SERVER_CONF_KEY, sslServerConfFile);
    sslConf.set(SSLFactory.SSL_CLIENT_CONF_KEY, sslClientConfFile);
    return sslConf;
  }

  @AfterClass
  public static void tearDown() {
    if (kdc != null) {
      kdc.stop();
    }

    FileUtil.fullyDelete(new File(HTU.getDataTestDir().toString()));
  }

  /**
   * Test client command line options. Does not validate server behavior.
   * @throws Exception if commands return unexpected results.
   */
  @Test
  public void testCommandOptions() throws Exception {
    final String className = this.getClass().getName();

    assertFalse(validateCommand(new String[] { "-foo" }));
    // fail due to insufficient number of arguments
    assertFalse(validateCommand(new String[] {}));
    assertFalse(validateCommand(new String[] { "-getlevel" }));
    assertFalse(validateCommand(new String[] { "-setlevel" }));
    assertFalse(validateCommand(new String[] { "-getlevel", "foo.bar:8080" }));

    // valid command arguments
    assertTrue(validateCommand(new String[] { "-getlevel", "foo.bar:8080", className }));
    assertTrue(validateCommand(new String[] { "-setlevel", "foo.bar:8080", className, "DEBUG" }));
    assertTrue(validateCommand(new String[] { "-getlevel", "foo.bar:8080", className }));
    assertTrue(validateCommand(new String[] { "-setlevel", "foo.bar:8080", className, "DEBUG" }));

    // fail due to the extra argument
    assertFalse(validateCommand(new String[] { "-getlevel", "foo.bar:8080", className, "blah" }));
    assertFalse(
      validateCommand(new String[] { "-setlevel", "foo.bar:8080", className, "DEBUG", "blah" }));
    assertFalse(validateCommand(new String[] { "-getlevel", "foo.bar:8080", className, "-setlevel",
      "foo.bar:8080", className }));
  }

  /**
   * Check to see if a command can be accepted.
   * @param args a String array of arguments
   * @return true if the command can be accepted, false if not.
   */
  private boolean validateCommand(String[] args) {
    CLI cli = new CLI(clientConf);
    try {
      cli.parseArguments(args);
    } catch (HadoopIllegalArgumentException e) {
      return false;
    } catch (Exception e) {
      // this is used to verify the command arguments only.
      // no HadoopIllegalArgumentException = the arguments are good.
      return true;
    }
    return true;
  }

  /**
   * Creates and starts a Jetty server binding at an ephemeral port to run LogLevel servlet.
   * @param protocol "http" or "https"
   * @param isSpnego true if SPNEGO is enabled
   * @return a created HttpServer object
   * @throws Exception if unable to create or start a Jetty server
   */
  private HttpServer createServer(String protocol, boolean isSpnego) throws Exception {
    // Changed to "" as ".." moves it a steps back in path because the path is relative to the
    // current working directory. throws "java.lang.IllegalArgumentException: Base Resource is not
    // valid: hbase-http/target/test-classes/static" as it is not able to find the static folder.
    HttpServer.Builder builder = new HttpServer.Builder().setName("")
      .addEndpoint(new URI(protocol + "://localhost:0")).setFindPort(true).setConf(serverConf);
    if (isSpnego) {
      // Set up server Kerberos credentials.
      // Since the server may fall back to simple authentication,
      // use ACL to make sure the connection is Kerberos/SPNEGO authenticated.
      builder.setSecurityEnabled(true).setUsernameConfKey(PRINCIPAL).setKeytabConfKey(KEYTAB)
        .setACL(new AccessControlList("client"));
    }

    // if using HTTPS, configure keystore/truststore properties.
    if (protocol.equals(LogLevel.PROTOCOL_HTTPS)) {
      builder = builder.keyPassword(sslConf.get("ssl.server.keystore.keypassword"))
        .keyStore(sslConf.get("ssl.server.keystore.location"),
          sslConf.get("ssl.server.keystore.password"),
          sslConf.get("ssl.server.keystore.type", "jks"))
        .trustStore(sslConf.get("ssl.server.truststore.location"),
          sslConf.get("ssl.server.truststore.password"),
          sslConf.get("ssl.server.truststore.type", "jks"));
    }

    HttpServer server = builder.build();
    server.start();
    return server;
  }

  private void testGetLogLevel(Protocol protocol, boolean isSpnego, String loggerName,
    String expectedLevel) throws Exception {
    withLogLevelServer(protocol, isSpnego, (authority) -> {
      final String level = getLevel(protocol.client, authority, loggerName);
      assertEquals("Log level not equal to expected: ", expectedLevel, level);
    });
  }

  private void testSetLogLevel(Protocol protocol, boolean isSpnego, String loggerName,
    String newLevel) throws Exception {
    String oldLevel = Log4jUtils.getEffectiveLevel(loggerName);
    assertNotEquals("New level is same as old level: ", newLevel, oldLevel);

    try {
      withLogLevelServer(protocol, isSpnego, (authority) -> {
        setLevel(protocol.client, authority, loggerName, newLevel);
      });
    } finally {
      // restore log level
      Log4jUtils.setLogLevel(loggerName, oldLevel);
    }
  }

  /**
   * Starts a LogLevel server and executes a client action against it.
   * @param protocol protocol configuration for client and server
   * @param isSpnego whether SPNEGO authentication is enabled
   * @param consumer client action executed with the server authority (host:port)
   * @throws Exception if server setup or client execution fails
   */
  private void withLogLevelServer(Protocol protocol, final boolean isSpnego,
    ThrowingConsumer consumer) throws Exception {
    if (!LogLevel.isValidProtocol(protocol.server)) {
      throw new Exception("Invalid server protocol " + protocol.server);
    }
    if (!LogLevel.isValidProtocol(protocol.client)) {
      throw new Exception("Invalid client protocol " + protocol.client);
    }

    // configs needed for SPNEGO at server side
    if (isSpnego) {
      serverConf.set(PRINCIPAL, HTTP_PRINCIPAL);
      serverConf.set(KEYTAB, keyTabFile.getAbsolutePath());
      serverConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      serverConf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
      UserGroupInformation.setConfiguration(serverConf);
    } else {
      serverConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "simple");
      serverConf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation.setConfiguration(serverConf);
    }

    final HttpServer server = createServer(protocol.server, isSpnego);
    final String authority = NetUtils.getHostPortString(server.getConnectorAddress(0));
    String keytabFilePath = keyTabFile.getAbsolutePath();

    UserGroupInformation clientUGI =
      UserGroupInformation.loginUserFromKeytabAndReturnUGI(clientPrincipal, keytabFilePath);
    try {
      clientUGI.doAs((PrivilegedExceptionAction<Void>) () -> {
        consumer.accept(authority);
        return null;
      });
    } finally {
      clientUGI.logoutUserFromKeytab();
      server.stop();
    }
  }

  /**
   * Run LogLevel command line to start a client to get log level of this test class.
   * @param protocol  specify either http or https
   * @param authority daemon's web UI address
   * @throws Exception if unable to connect
   */
  private String getLevel(String protocol, String authority, String logName) throws Exception {
    String[] getLevelArgs = { "-getlevel", authority, logName, "-protocol", protocol };
    CLI cli = new CLI(protocol.equalsIgnoreCase("https") ? sslConf : clientConf);
    cli.parseArguments(getLevelArgs);
    final String response = cli.fetchGetLevelResponse();
    return extractEffectiveLevel(response);
  }

  /**
   * Run LogLevel command line to start a client to set log level of this test class to debug.
   * @param protocol  specify either http or https
   * @param authority daemon's web UI address
   * @throws Exception if unable to run or log level does not change as expected
   */
  private void setLevel(String protocol, String authority, String logName, String newLevel)
    throws Exception {
    String[] setLevelArgs = { "-setlevel", authority, logName, newLevel, "-protocol", protocol };
    CLI cli = new CLI(protocol.equalsIgnoreCase("https") ? sslConf : clientConf);
    cli.parseArguments(setLevelArgs);
    final String response = cli.fetchSetLevelResponse();
    final String responseLevel = extractEffectiveLevel(response);
    final String currentLevel = Log4jUtils.getEffectiveLevel(logName);
    assertEquals("new level not equal to expected: ", newLevel, currentLevel);
    assertSame("new level not equal to response level: ", newLevel, responseLevel);
  }

  /**
   * Extract effective log level from server response.
   * @param response server body response string
   * @return the effective log level
   */
  private String extractEffectiveLevel(String response) {
    Matcher m = EFFECTIVE_LEVEL.matcher(response);
    if (m.find()) {
      return org.apache.logging.log4j.Level.toLevel(m.group(1)).name();
    }

    fail("Cannot find effective log level from response: " + response);
    return null;
  }

  @Test
  public void testSettingProtectedLogLevel() throws Exception {
    try {
      testSetLogLevel(Protocol.C_HTTP_S_HTTP, true, protectedLogName, "DEBUG");
      fail("Expected IO exception due to protected logger");
    } catch (IOException e) {
      assertTrue(e.getMessage().contains("" + HttpServletResponse.SC_PRECONDITION_FAILED));
      assertTrue(e.getMessage().contains(
        "Modification of logger " + protectedLogName + " is disallowed in configuration."));
    }
  }

  @Test
  public void testGetDebugLogLevel() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testGetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "DEBUG");
  }

  @Test
  public void testGetInfoLogLevel() throws Exception {
    Log4jUtils.setLogLevel(logName, "INFO");
    testGetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "INFO");
  }

  /**
   * Test setting log level to "Info".
   * @throws Exception if client can't set log level to INFO.
   */
  @Test
  public void testSetInfoLogLevel() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testSetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "INFO");
  }

  /**
   * Test setting log level to "Error".
   * @throws Exception if client can't set log level to ERROR.
   */
  @Test
  public void testSetErrorLogLevel() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testSetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "ERROR");
  }

  /**
   * Server runs HTTP, no SPNEGO.
   * @throws Exception if http client can't access http server, or http client can access https
   *                   server.
   */
  @Test
  public void testLogLevelByHttp() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testGetLogLevel(Protocol.C_HTTP_S_HTTP, false, logName, "DEBUG");
    try {
      testGetLogLevel(Protocol.C_HTTPS_S_HTTP, false, logName, "DEBUG");
      fail("An HTTPS Client should not have succeeded in connecting to a HTTP server");
    } catch (SSLException e) {
      exceptionShouldContains("Unrecognized SSL message", e);
    }
  }

  /**
   * Server runs HTTP + SPNEGO.
   * @throws Exception if http client can't access http server, or http client can access https
   *                   server.
   */
  @Test
  public void testLogLevelByHttpWithSpnego() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testGetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "DEBUG");
    try {
      testGetLogLevel(Protocol.C_HTTPS_S_HTTP, true, logName, "DEBUG");
      fail("An HTTPS Client should not have succeeded in connecting to a HTTP server");
    } catch (SSLException e) {
      exceptionShouldContains("Unrecognized SSL message", e);
    }
  }

  /**
   * Server runs HTTPS, no SPNEGO.
   * @throws Exception if https client can't access https server, or https client can access http
   *                   server.
   */
  @Test
  public void testLogLevelByHttps() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testGetLogLevel(Protocol.C_HTTPS_S_HTTPS, false, logName, "DEBUG");
    try {
      testGetLogLevel(Protocol.C_HTTP_S_HTTPS, false, logName, "DEBUG");
      fail("An HTTP Client should not have succeeded in connecting to a HTTPS server");
    } catch (SocketException e) {
      exceptionShouldContains("Unexpected end of file from server", e);
    }
  }

  /**
   * Server runs HTTPS + SPNEGO.
   * @throws Exception if https client can't access https server, or https client can access http
   *                   server.
   */
  @Test
  public void testLogLevelByHttpsWithSpnego() throws Exception {
    Log4jUtils.setLogLevel(logName, "DEBUG");
    testGetLogLevel(Protocol.C_HTTPS_S_HTTPS, true, logName, "DEBUG");
    try {
      testGetLogLevel(Protocol.C_HTTP_S_HTTPS, true, logName, "DEBUG");
      fail("An HTTP Client should not have succeeded in connecting to a HTTPS server");
    } catch (SocketException e) {
      exceptionShouldContains("Unexpected end of file from server", e);
    }
  }

  /**
   * Test getting log level in readonly mode.
   * @throws Exception if a client can't get log level.
   */
  @Test
  public void testGetLogLevelAllowedInReadonlyMode() throws Exception {
    withMasterUIReadonly(() -> {
      Log4jUtils.setLogLevel(logName, "DEBUG");
      testGetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "DEBUG");
    });
  }

  /**
   * Test setting log level in readonly mode.
   * @throws Exception if a client can set log level.
   */
  @Test
  public void testSetLogLevelDisallowedInReadonlyMode() throws Exception {
    withMasterUIReadonly(() -> {
      Log4jUtils.setLogLevel(logName, "DEBUG");
      try {
        testSetLogLevel(Protocol.C_HTTP_S_HTTP, true, logName, "INFO");
        fail("Setting log level should be disallowed in readonly mode.");
      } catch (IOException e) {
        exceptionShouldContains("Modification of HBase via the UI is disallowed in configuration.",
          e);
      }
    });
  }

  private void withMasterUIReadonly(ThrowingRunnable runnable) throws Exception {
    boolean prev = serverConf.getBoolean(LogLevel.MASTER_UI_READONLY_CONF_KEY, false);
    serverConf.setBoolean(LogLevel.MASTER_UI_READONLY_CONF_KEY, true);
    try {
      runnable.run();
    } finally {
      serverConf.setBoolean(LogLevel.MASTER_UI_READONLY_CONF_KEY, prev);
    }
  }

  /**
   * Assert that a throwable or one of its causes should contain the substr in its message. Ideally
   * we should use {@link GenericTestUtils#assertExceptionContains(String, Throwable)} util method
   * which asserts t.toString() contains the substr. As the original throwable may have been wrapped
   * in Hadoop3 because of HADOOP-12897, it's required to check all the wrapped causes. After stop
   * supporting Hadoop2, this method can be removed and assertion in tests can use t.getCause()
   * directly, similar to HADOOP-15280.
   */
  private static void exceptionShouldContains(String substr, Throwable throwable) {
    Throwable t = throwable;
    while (t != null) {
      String msg = t.toString();
      if (msg != null && msg.toLowerCase().contains(substr.toLowerCase())) {
        return;
      }
      t = t.getCause();
    }
    throw new AssertionError("Expected to find '" + substr + "' but got unexpected exception:"
      + StringUtils.stringifyException(throwable), throwable);
  }
}
