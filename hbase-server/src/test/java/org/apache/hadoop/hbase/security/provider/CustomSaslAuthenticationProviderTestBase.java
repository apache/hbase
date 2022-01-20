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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;
import javax.security.sasl.RealmChoiceCallback;
import javax.security.sasl.Sasl;
import javax.security.sasl.SaslClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.LocalHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNameTestRule;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.ipc.BlockingRpcClient;
import org.apache.hadoop.hbase.ipc.NettyRpcClient;
import org.apache.hadoop.hbase.ipc.RpcClientFactory;
import org.apache.hadoop.hbase.ipc.RpcServerFactory;
import org.apache.hadoop.hbase.security.AccessDeniedException;
import org.apache.hadoop.hbase.security.HBaseKerberosUtils;
import org.apache.hadoop.hbase.security.SaslUtil;
import org.apache.hadoop.hbase.security.SecurityInfo;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.token.SecureTestCluster;
import org.apache.hadoop.hbase.security.token.TokenProvider;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.UserGroupInformation.AuthenticationMethod;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.SecretManager.InvalidToken;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.base.Throwables;

import org.apache.hadoop.hbase.shaded.protobuf.generated.RPCProtos.UserInformation;

/**
 * Tests the pluggable authentication framework with SASL using a contrived authentication system.
 * This tests holds a "user database" in memory as a hashmap. Clients provide their password in the
 * client Hadoop configuration. The servers validate this password via the "user database".
 */
public abstract class CustomSaslAuthenticationProviderTestBase {

  private static final Logger LOG =
    LoggerFactory.getLogger(CustomSaslAuthenticationProviderTestBase.class);

  private static final Map<String, String> USER_DATABASE = createUserDatabase();

  private static final String USER1_PASSWORD = "foobarbaz";
  private static final String USER2_PASSWORD = "bazbarfoo";

  @Parameters
  public static Collection<Object[]> parameters() {
    return Arrays.asList(new Object[] { BlockingRpcClient.class.getName() },
      new Object[] { NettyRpcClient.class.getName() });
  }

  @Parameter
  public String rpcClientImpl;

  private static Map<String, String> createUserDatabase() {
    Map<String, String> db = new ConcurrentHashMap<>();
    db.put("user1", USER1_PASSWORD);
    db.put("user2", USER2_PASSWORD);
    return db;
  }

  public static String getPassword(String user) {
    String password = USER_DATABASE.get(user);
    if (password == null) {
      throw new IllegalStateException("Cannot request password for a user that doesn't exist");
    }
    return password;
  }

  /**
   * A custom token identifier for our custom auth'n method. Unique from the TokenIdentifier used
   * for delegation tokens.
   */
  public static class PasswordAuthTokenIdentifier extends TokenIdentifier {
    public static final Text PASSWORD_AUTH_TOKEN = new Text("HBASE_PASSWORD_TEST_TOKEN");
    private String username;

    public PasswordAuthTokenIdentifier() {
    }

    public PasswordAuthTokenIdentifier(String username) {
      this.username = username;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.username = WritableUtils.readString(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      WritableUtils.writeString(out, username);
    }

    @Override
    public Text getKind() {
      return PASSWORD_AUTH_TOKEN;
    }

    @Override
    public UserGroupInformation getUser() {
      if (username == null || "".equals(username)) {
        return null;
      }
      return UserGroupInformation.createRemoteUser(username);
    }
  }

  public static Token<? extends TokenIdentifier> createPasswordToken(String username,
    String password, String clusterId) {
    PasswordAuthTokenIdentifier id = new PasswordAuthTokenIdentifier(username);
    Token<? extends TokenIdentifier> token =
      new Token<>(id.getBytes(), Bytes.toBytes(password), id.getKind(), new Text(clusterId));
    return token;
  }

  /**
   * Client provider that finds custom Token in the user's UGI and authenticates with the server via
   * DIGEST-MD5 using that password.
   */
  public static class InMemoryClientProvider extends AbstractSaslClientAuthenticationProvider {
    public static final String MECHANISM = "DIGEST-MD5";
    public static final SaslAuthMethod SASL_AUTH_METHOD =
      new SaslAuthMethod("IN_MEMORY", (byte) 42, MECHANISM, AuthenticationMethod.TOKEN);

    @Override
    public SaslClient createClient(Configuration conf, InetAddress serverAddr,
      SecurityInfo securityInfo, Token<? extends TokenIdentifier> token, boolean fallbackAllowed,
      Map<String, String> saslProps) throws IOException {
      return Sasl.createSaslClient(new String[] { MECHANISM }, null, null,
        SaslUtil.SASL_DEFAULT_REALM, saslProps, new InMemoryClientProviderCallbackHandler(token));
    }

    public Optional<Token<? extends TokenIdentifier>> findToken(User user) {
      List<Token<? extends TokenIdentifier>> tokens = user.getTokens().stream()
        .filter((token) -> token.getKind().equals(PasswordAuthTokenIdentifier.PASSWORD_AUTH_TOKEN))
        .collect(Collectors.toList());
      if (tokens.isEmpty()) {
        return Optional.empty();
      }
      if (tokens.size() > 1) {
        throw new IllegalStateException("Cannot handle more than one PasswordAuthToken");
      }
      return Optional.of(tokens.get(0));
    }

    @Override
    public SaslAuthMethod getSaslAuthMethod() {
      return SASL_AUTH_METHOD;
    }

    /**
     * Sasl CallbackHandler which extracts information from our custom token and places it into the
     * Sasl objects.
     */
    public class InMemoryClientProviderCallbackHandler implements CallbackHandler {
      private final Token<? extends TokenIdentifier> token;

      public InMemoryClientProviderCallbackHandler(Token<? extends TokenIdentifier> token) {
        this.token = token;
      }

      @Override
      public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        RealmCallback rc = null;
        for (Callback callback : callbacks) {
          if (callback instanceof RealmChoiceCallback) {
            continue;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (callback instanceof RealmCallback) {
            rc = (RealmCallback) callback;
          } else {
            throw new UnsupportedCallbackException(callback, "Unrecognized SASL client callback");
          }
        }
        if (nc != null) {
          nc.setName(SaslUtil.encodeIdentifier(token.getIdentifier()));
        }
        if (pc != null) {
          pc.setPassword(SaslUtil.encodePassword(token.getPassword()));
        }
        if (rc != null) {
          rc.setText(rc.getDefaultText());
        }
      }
    }

    @Override
    public UserInformation getUserInfo(User user) {
      return null;
    }
  }

  /**
   * Server provider which validates credentials from an in-memory database.
   */
  public static class InMemoryServerProvider extends InMemoryClientProvider
    implements SaslServerAuthenticationProvider {

    @Override
    public AttemptingUserProvidingSaslServer
      createServer(SecretManager<TokenIdentifier> secretManager, Map<String, String> saslProps)
        throws IOException {
      return new AttemptingUserProvidingSaslServer(
        Sasl.createSaslServer(getSaslAuthMethod().getSaslMechanism(), null,
          SaslUtil.SASL_DEFAULT_REALM, saslProps, new InMemoryServerProviderCallbackHandler()),
        () -> null);
    }

    /**
     * Pulls the correct password for the user who started the SASL handshake so that SASL can
     * validate that the user provided the right password.
     */
    private class InMemoryServerProviderCallbackHandler implements CallbackHandler {

      @Override
      public void handle(Callback[] callbacks) throws InvalidToken, UnsupportedCallbackException {
        NameCallback nc = null;
        PasswordCallback pc = null;
        AuthorizeCallback ac = null;
        for (Callback callback : callbacks) {
          if (callback instanceof AuthorizeCallback) {
            ac = (AuthorizeCallback) callback;
          } else if (callback instanceof NameCallback) {
            nc = (NameCallback) callback;
          } else if (callback instanceof PasswordCallback) {
            pc = (PasswordCallback) callback;
          } else if (callback instanceof RealmCallback) {
            continue; // realm is ignored
          } else {
            throw new UnsupportedCallbackException(callback, "Unrecognized SASL Callback");
          }
        }
        if (nc != null && pc != null) {
          byte[] encodedId = SaslUtil.decodeIdentifier(nc.getDefaultName());
          PasswordAuthTokenIdentifier id = new PasswordAuthTokenIdentifier();
          try {
            id.readFields(new DataInputStream(new ByteArrayInputStream(encodedId)));
          } catch (IOException e) {
            throw (InvalidToken) new InvalidToken("Can't de-serialize tokenIdentifier")
              .initCause(e);
          }
          char[] actualPassword =
            SaslUtil.encodePassword(Bytes.toBytes(getPassword(id.getUser().getUserName())));
          pc.setPassword(actualPassword);
        }
        if (ac != null) {
          String authid = ac.getAuthenticationID();
          String authzid = ac.getAuthorizationID();
          if (authid.equals(authzid)) {
            ac.setAuthorized(true);
          } else {
            ac.setAuthorized(false);
          }
          if (ac.isAuthorized()) {
            ac.setAuthorizedID(authzid);
          }
        }
      }
    }

    @Override
    public boolean supportsProtocolAuthentication() {
      return false;
    }

    @Override
    public UserGroupInformation getAuthorizedUgi(String authzId,
      SecretManager<TokenIdentifier> secretManager) throws IOException {
      UserGroupInformation authorizedUgi;
      byte[] encodedId = SaslUtil.decodeIdentifier(authzId);
      PasswordAuthTokenIdentifier tokenId = new PasswordAuthTokenIdentifier();
      try {
        tokenId.readFields(new DataInputStream(new ByteArrayInputStream(encodedId)));
      } catch (IOException e) {
        throw new IOException("Can't de-serialize PasswordAuthTokenIdentifier", e);
      }
      authorizedUgi = tokenId.getUser();
      if (authorizedUgi == null) {
        throw new AccessDeniedException("Can't retrieve username from tokenIdentifier.");
      }
      authorizedUgi.addTokenIdentifier(tokenId);
      authorizedUgi.setAuthenticationMethod(getSaslAuthMethod().getAuthMethod());
      return authorizedUgi;
    }
  }

  /**
   * Custom provider which can select our custom provider, amongst other tokens which may be
   * available.
   */
  public static class InMemoryProviderSelector extends BuiltInProviderSelector {
    private InMemoryClientProvider inMemoryProvider;

    @Override
    public void configure(Configuration conf,
      Collection<SaslClientAuthenticationProvider> providers) {
      super.configure(conf, providers);
      Optional<SaslClientAuthenticationProvider> o =
        providers.stream().filter((p) -> p instanceof InMemoryClientProvider).findAny();

      inMemoryProvider = (InMemoryClientProvider) o.orElseThrow(() -> new RuntimeException(
        "InMemoryClientProvider not found in available providers: " + providers));
    }

    @Override
    public Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>>
      selectProvider(String clusterId, User user) {
      Pair<SaslClientAuthenticationProvider, Token<? extends TokenIdentifier>> superPair =
        super.selectProvider(clusterId, user);

      Optional<Token<? extends TokenIdentifier>> optional = inMemoryProvider.findToken(user);
      if (optional.isPresent()) {
        LOG.info("Using InMemoryClientProvider");
        return new Pair<>(inMemoryProvider, optional.get());
      }

      LOG.info("InMemoryClientProvider not usable, falling back to {}", superPair);
      return superPair;
    }
  }

  private static void createBaseCluster(HBaseTestingUtility util, File keytabFile, MiniKdc kdc)
    throws Exception {
    String servicePrincipal = "hbase/localhost";
    String spnegoPrincipal = "HTTP/localhost";
    kdc.createPrincipal(keytabFile, servicePrincipal);
    util.startMiniZKCluster();

    HBaseKerberosUtils.setSecuredConfiguration(util.getConfiguration(),
      servicePrincipal + "@" + kdc.getRealm(), spnegoPrincipal + "@" + kdc.getRealm());
    HBaseKerberosUtils.setSSLConfiguration(util, SecureTestCluster.class);

    util.getConfiguration().setStrings(CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      TokenProvider.class.getName());
    util.startMiniDFSCluster(1);
    Path rootdir = util.getDataTestDirOnTestFS("TestCustomSaslAuthenticationProvider");
    CommonFSUtils.setRootDir(util.getConfiguration(), rootdir);
  }

  private static final HBaseTestingUtility UTIL = new HBaseTestingUtility();
  private static final Configuration CONF = UTIL.getConfiguration();
  private static LocalHBaseCluster CLUSTER;
  private static File KEYTAB_FILE;

  protected static void startCluster(String rpcServerImpl) throws Exception {
    KEYTAB_FILE = new File(UTIL.getDataTestDir("keytab").toUri().getPath());
    final MiniKdc kdc = UTIL.setupMiniKdc(KEYTAB_FILE);

    // Adds our test impls instead of creating service loader entries which
    // might inadvertently get them loaded on a real cluster.
    CONF.setStrings(SaslClientAuthenticationProviders.EXTRA_PROVIDERS_KEY,
      InMemoryClientProvider.class.getName());
    CONF.setStrings(SaslServerAuthenticationProviders.EXTRA_PROVIDERS_KEY,
      InMemoryServerProvider.class.getName());
    CONF.set(SaslClientAuthenticationProviders.SELECTOR_KEY,
      InMemoryProviderSelector.class.getName());
    CONF.setLong(HADOOP_KERBEROS_MIN_SECONDS_BEFORE_RELOGIN, 600);
    createBaseCluster(UTIL, KEYTAB_FILE, kdc);
    CONF.set(RpcServerFactory.CUSTOM_RPC_SERVER_IMPL_CONF_KEY, rpcServerImpl);
    CLUSTER = new LocalHBaseCluster(CONF, 1);
    CLUSTER.startup();
  }

  @AfterClass
  public static void shutdownCluster() throws Exception {
    if (CLUSTER != null) {
      CLUSTER.shutdown();
      CLUSTER = null;
    }
    UTIL.shutdownMiniDFSCluster();
    UTIL.shutdownMiniZKCluster();
    UTIL.cleanupTestDir();
  }

  @Before
  public void setUp() throws Exception {
    createTable();
  }

  @After
  public void tearDown() throws IOException {
    UTIL.deleteTable(name.getTableName());
  }

  @Rule
  public TableNameTestRule name = new TableNameTestRule();

  private TableName tableName;

  private String clusterId;

  private void createTable() throws Exception {
    tableName = name.getTableName();

    // Create a table and write a record as the service user (hbase)
    UserGroupInformation serviceUgi = UserGroupInformation
      .loginUserFromKeytabAndReturnUGI("hbase/localhost", KEYTAB_FILE.getAbsolutePath());
    clusterId = serviceUgi.doAs(new PrivilegedExceptionAction<String>() {
      @Override
      public String run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(CONF);
          Admin admin = conn.getAdmin();) {
          admin.createTable(TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(ColumnFamilyDescriptorBuilder.of("f1")).build());

          UTIL.waitTableAvailable(tableName);

          try (Table t = conn.getTable(tableName)) {
            Put p = new Put(Bytes.toBytes("r1"));
            p.addColumn(Bytes.toBytes("f1"), Bytes.toBytes("q1"), Bytes.toBytes("1"));
            t.put(p);
          }

          return admin.getClusterMetrics().getClusterId();
        }
      }
    });
    assertNotNull(clusterId);
  }

  private Configuration getClientConf() {
    Configuration conf = new Configuration(CONF);
    conf.set(RpcClientFactory.CUSTOM_RPC_CLIENT_IMPL_CONF_KEY, rpcClientImpl);
    return conf;
  }

  @Test
  public void testPositiveAuthentication() throws Exception {
    // Validate that we can read that record back out as the user with our custom auth'n
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting("user1", new String[0]);
    user1.addToken(createPasswordToken("user1", USER1_PASSWORD, clusterId));
    user1.doAs(new PrivilegedExceptionAction<Void>() {
      @Override
      public Void run() throws Exception {
        try (Connection conn = ConnectionFactory.createConnection(getClientConf());
          Table t = conn.getTable(tableName)) {
          Result r = t.get(new Get(Bytes.toBytes("r1")));
          assertNotNull(r);
          assertFalse("Should have read a non-empty Result", r.isEmpty());
          final Cell cell = r.getColumnLatestCell(Bytes.toBytes("f1"), Bytes.toBytes("q1"));
          assertTrue("Unexpected value", CellUtil.matchingValue(cell, Bytes.toBytes("1")));

          return null;
        }
      }
    });
  }

  @Test
  public void testNegativeAuthentication() throws Exception {
    // Validate that we can read that record back out as the user with our custom auth'n
    final Configuration clientConf = new Configuration(CONF);
    // This test does not work with master registry in branch-2 because of a nuance in the non-async
    // connection implementation. See the detail below.
    clientConf.set(HConstants.CLIENT_CONNECTION_REGISTRY_IMPL_CONF_KEY,
        HConstants.ZK_CONNECTION_REGISTRY_CLASS);
    clientConf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3);
    UserGroupInformation user1 = UserGroupInformation.createUserForTesting(
        "user1", new String[0]);
    user1.addToken(createPasswordToken("user1", "definitely not the password", clusterId));
    user1.doAs(new PrivilegedExceptionAction<Void>() {
      @Override public Void run() throws Exception {
        // There is a slight behavioral difference here in the 3.x vs 2.x branches. 3.x branches
        // use async client connection implementation which throws if there is an exception when
        // fetching the clusterId(). 2.x branches that use non-async client falls back to using a
        // DEFAULT cluster ID in such cases. 3.x behavior makes more sense, especially if the
        // exception is of type InvalidToken (digest mis-match), however I did not want to fix it
        // since it makes sense only when master registry is in use (which has RPCs to master).
        // That is the reason if you see a slight difference in the test between 3.x and 2.x.
        try (Connection conn = ConnectionFactory.createConnection(clientConf);
          Table t = conn.getTable(tableName)) {
          t.get(new Get(Bytes.toBytes("r1")));
          fail("Should not successfully authenticate with HBase");
        } catch (RetriesExhaustedException re) {
          assertTrue(re.getMessage(), re.getMessage().contains("SaslException"));
        } catch (Exception e) {
          // Any other exception is unexpected.
          fail("Unexpected exception caught, was expecting a authentication error: " +
            Throwables.getStackTraceAsString(e));
        }
        return null;
      }
    });
  }
}
