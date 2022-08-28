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
package org.apache.hadoop.hbase.security.token;

import static org.junit.Assert.assertArrayEquals;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.KeyUse;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.time.LocalDate;
import java.util.UUID;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ SecurityTests.class, MediumTests.class })
public class TestOAuthBearerAuthentication extends SecureTestCluster {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestOAuthBearerAuthentication.class);

  private static final String AUDIENCE = "valid-hbase-instance";
  private static final String ISSUER = "authorized-issuer";

  private static RSAKey RSA;
  private static File JWKS_FILE;

  @BeforeClass
  public static void setUp() throws Exception {
    initRSA();

    TEST_UTIL.getConfiguration().set("hbase.client.sasl.provider.extras",
      "org.apache.hadoop.hbase.security.provider.OAuthBearerSaslClientAuthenticationProvider");
    TEST_UTIL.getConfiguration().set("hbase.server.sasl.provider.extras",
      "org.apache.hadoop.hbase.security.provider.OAuthBearerSaslServerAuthenticationProvider");
    TEST_UTIL.getConfiguration().set("hbase.client.sasl.provider.class",
      "org.apache.hadoop.hbase.security.provider.OAuthBearerSaslProviderSelector");
    TEST_UTIL.getConfiguration().set("hbase.security.oauth.jwt.jwks.file",
      JWKS_FILE.getAbsolutePath());
    TEST_UTIL.getConfiguration().set("hbase.security.oauth.jwt.audience", AUDIENCE);
    TEST_UTIL.getConfiguration().set("hbase.security.oauth.jwt.issuer", ISSUER);

    SecureTestCluster.setUp();
  }

  @Rule
  public TestName testName = new TestName();

  private static void initRSA() throws JOSEException, IOException {
    RSA = new RSAKeyGenerator(2048).keyUse(KeyUse.SIGNATURE) // indicate the intended use of the key
      .keyID(UUID.randomUUID().toString()) // give the key a unique ID
      .generate();
    JWKSet jwkSet = new JWKSet(RSA.toPublicJWK());
    JWKS_FILE = File.createTempFile("oauth_", ".jwks");
    JWKS_FILE.deleteOnExit();

    try (OutputStream os = new FileOutputStream(JWKS_FILE);
      OutputStreamWriter osw = new OutputStreamWriter(os)) {
      osw.write(jwkSet.toString(true));
    }
  }

  private String generateBase64EncodedToken(String principal) throws JOSEException {
    JWSSigner signer = new RSASSASigner(RSA);
    LocalDate now = LocalDate.now();

    JWTClaimsSet claimsSet = new JWTClaimsSet.Builder().subject(principal).issuer(ISSUER)
      .audience(AUDIENCE).expirationTime(java.sql.Date.valueOf(now.plusDays(1))).build();

    SignedJWT signedJWT = new SignedJWT(
      new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(RSA.getKeyID()).build(), claimsSet);

    signedJWT.sign(signer);

    return signedJWT.serialize();
  }

  private TableName getTestTableName() {
    return TableName.valueOf(testName.getMethodName().replaceAll("[^0-9A-Za-z]", "_"));
  }

  @Test
  public void testOAuthBearerLogin() throws IOException, JOSEException {
    TableName tableName = getTestTableName();
    byte[] family = Bytes.toBytes("f");
    byte[] qualifier = Bytes.toBytes("q");
    byte[] row = Bytes.toBytes("row");
    byte[] value = Bytes.toBytes("data");

    User user =
      User.createUserForTesting(TEST_UTIL.getConfiguration(), "testuser_jwt", new String[] {});
    OAuthBearerTokenUtil.addTokenForUser(user, generateBase64EncodedToken(user.getName()), 0);

    try (Connection conn = ConnectionFactory.createConnection(TEST_UTIL.getConfiguration(), user)) {
      Admin admin = conn.getAdmin();
      TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
        .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family)).build();
      admin.createTable(tableDescriptor);
      try (Table table = conn.getTable(tableName)) {
        table.put(new Put(row).addColumn(family, qualifier, value));
        Result result = table.get(new Get(row));
        assertArrayEquals(value, result.getValue(family, qualifier));
      }
    }
  }
}
