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
package org.apache.hadoop.hbase.http.log;

import static org.apache.hadoop.hbase.http.log.LogLevel.PROTOCOL_HTTP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.BindException;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseCommonTestingUtility;
import org.apache.hadoop.hbase.http.HttpServer;
import org.apache.hadoop.hbase.http.log.LogLevel.CLI;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.AccessControlList;
import org.apache.hadoop.test.GenericTestUtils;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test LogLevel.
 */
@Category({MiscTests.class, SmallTests.class})
public class TestLogLevel {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestLogLevel.class);

  private static File BASEDIR;
  private static Configuration serverConf;
  private static Configuration clientConf;
  private static final String logName = TestLogLevel.class.getName();
  private static final Logger log = LogManager.getLogger(logName);
  private final static String PRINCIPAL = "loglevel.principal";
  private final static String KEYTAB  = "loglevel.keytab";

  private static MiniKdc kdc;
  private static HBaseCommonTestingUtility htu = new HBaseCommonTestingUtility();

  private static final String LOCALHOST = "localhost";
  private static final String clientPrincipal = "client/" + LOCALHOST;
  private static String HTTP_PRINCIPAL = "HTTP/" + LOCALHOST;

  private static final File KEYTAB_FILE = new File(
      htu.getDataTestDir("keytab").toUri().getPath());

  @BeforeClass
  public static void setUp() throws Exception {
    BASEDIR = new File(htu.getDataTestDir().toUri().getPath());

    FileUtil.fullyDelete(BASEDIR);
    if (!BASEDIR.mkdirs()) {
      throw new Exception("unable to create the base directory for testing");
    }
    serverConf = new Configuration();
    clientConf = new Configuration();

    kdc = setupMiniKdc();
    // Create two principles: a client and a HTTP principal
    kdc.createPrincipal(KEYTAB_FILE, clientPrincipal, HTTP_PRINCIPAL);
  }

  /**
   * Sets up {@link MiniKdc} for testing security.
   * Copied from HBaseTestingUtility#setupMiniKdc().
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
        dir = new File(htu.getDataTestDir("kdc").toUri().getPath());
        kdc = new MiniKdc(conf, dir);
        kdc.start();
      } catch (BindException e) {
        FileUtils.deleteDirectory(dir);  // clean directory
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

  @AfterClass
  public static void tearDown() {
    if (kdc != null) {
      kdc.stop();
    }

    FileUtil.fullyDelete(BASEDIR);
  }

  /**
   * Test client command line options. Does not validate server behavior.
   * @throws Exception if commands return unexpected results.
   */
  @Test(timeout=120000)
  public void testCommandOptions() throws Exception {
    final String className = this.getClass().getName();

    assertFalse(validateCommand(new String[] {"-foo" }));
    // fail due to insufficient number of arguments
    assertFalse(validateCommand(new String[] {}));
    assertFalse(validateCommand(new String[] {"-getlevel" }));
    assertFalse(validateCommand(new String[] {"-setlevel" }));
    assertFalse(validateCommand(new String[] {"-getlevel", "foo.bar:8080" }));

    // valid command arguments
    assertTrue(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className }));
    assertTrue(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG" }));
    assertTrue(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className }));
    assertTrue(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG" }));

    // fail due to the extra argument
    assertFalse(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className, "blah" }));
    assertFalse(validateCommand(
        new String[] {"-setlevel", "foo.bar:8080", className, "DEBUG", "blah" }));
    assertFalse(validateCommand(
        new String[] {"-getlevel", "foo.bar:8080", className, "-setlevel", "foo.bar:8080",
          className }));
  }

  /**
   * Check to see if a command can be accepted.
   *
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
   * Creates and starts a Jetty server binding at an ephemeral port to run
   * LogLevel servlet.
   * @param isSpnego true if SPNEGO is enabled
   * @return a created HttpServer object
   * @throws Exception if unable to create or start a Jetty server
   */
  private HttpServer createServer(boolean isSpnego)
      throws Exception {
    HttpServer.Builder builder = new HttpServer.Builder()
        .setName("..")
        .addEndpoint(new URI(PROTOCOL_HTTP + "://localhost:0"))
        .setFindPort(true)
        .setConf(serverConf);
    if (isSpnego) {
      // Set up server Kerberos credentials.
      // Since the server may fall back to simple authentication,
      // use ACL to make sure the connection is Kerberos/SPNEGO authenticated.
      builder.setSecurityEnabled(true)
          .setUsernameConfKey(PRINCIPAL)
          .setKeytabConfKey(KEYTAB)
          .setACL(new AccessControlList("client"));
    }

    HttpServer server = builder.build();
    server.start();
    return server;
  }

  private void testDynamicLogLevel(final boolean isSpnego)
      throws Exception {
    testDynamicLogLevel(isSpnego, Level.DEBUG.toString());
  }

  /**
   * Run both client and server using the given protocol.
   *
   * @param isSpnego true if SPNEGO is enabled
   * @throws Exception if client can't accesss server.
   */
  private void testDynamicLogLevel(final boolean isSpnego, final String newLevel)
      throws Exception {
    Level oldLevel = log.getEffectiveLevel();
    assertNotEquals("Get default Log Level which shouldn't be ERROR.",
        Level.ERROR, oldLevel);

    // configs needed for SPNEGO at server side
    if (isSpnego) {
      serverConf.set(PRINCIPAL, HTTP_PRINCIPAL);
      serverConf.set(KEYTAB, KEYTAB_FILE.getAbsolutePath());
      serverConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
      serverConf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, true);
      UserGroupInformation.setConfiguration(serverConf);
    } else {
      serverConf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION, "simple");
      serverConf.setBoolean(CommonConfigurationKeys.HADOOP_SECURITY_AUTHORIZATION, false);
      UserGroupInformation.setConfiguration(serverConf);
    }

    final HttpServer server = createServer(isSpnego);
    // get server port
    final String authority = NetUtils.getHostPortString(server.getConnectorAddress(0));

    String keytabFilePath = KEYTAB_FILE.getAbsolutePath();

    UserGroupInformation clientUGI = UserGroupInformation.
        loginUserFromKeytabAndReturnUGI(clientPrincipal, keytabFilePath);
    try {
      clientUGI.doAs((PrivilegedExceptionAction<Void>) () -> {
        // client command line
        getLevel(authority);
        setLevel(authority, newLevel);
        return null;
      });
    } finally {
      clientUGI.logoutUserFromKeytab();
      server.stop();
    }

    // restore log level
    GenericTestUtils.setLogLevel(log, oldLevel);
  }

  /**
   * Run LogLevel command line to start a client to get log level of this test
   * class.
   *
   * @param authority daemon's web UI address
   * @throws Exception if unable to connect
   */
  private void getLevel(String authority) throws Exception {
    String[] getLevelArgs = {"-getlevel", authority, logName};
    CLI cli = new CLI(clientConf);
    cli.run(getLevelArgs);
  }

  /**
   * Run LogLevel command line to start a client to set log level of this test
   * class to debug.
   *
   * @param authority daemon's web UI address
   * @throws Exception if unable to run or log level does not change as expected
   */
  private void setLevel(String authority, String newLevel)
      throws Exception {
    String[] setLevelArgs = {"-setlevel", authority, logName, newLevel};
    CLI cli = new CLI(clientConf);
    cli.run(setLevelArgs);

    assertEquals("new level not equal to expected: ", newLevel.toUpperCase(),
        log.getEffectiveLevel().toString());
  }

  /**
   * Test setting log level to "Info".
   *
   * @throws Exception if client can't set log level to INFO.
   */
  @Test(timeout=60000)
  public void testInfoLogLevel() throws Exception {
    testDynamicLogLevel(true, "INFO");
  }

  /**
   * Test setting log level to "Error".
   *
   * @throws Exception if client can't set log level to ERROR.
   */
  @Test(timeout=60000)
  public void testErrorLogLevel() throws Exception {
    testDynamicLogLevel(true, "ERROR");
  }

  /**
   * Server runs HTTP, no SPNEGO.
   *
   * @throws Exception if http client can't access http server.
   */
  @Test(timeout=60000)
  public void testLogLevelByHttp() throws Exception {
    testDynamicLogLevel(false);
  }

  /**
   * Server runs HTTP + SPNEGO.
   *
   * @throws Exception if http client can't access http server.
   */
  @Test(timeout=60000)
  public void testLogLevelByHttpWithSpnego() throws Exception {
    testDynamicLogLevel(true);
  }
}
