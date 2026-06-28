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
package org.apache.hadoop.hbase.io.crypto.tls;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import java.security.KeyStore;
import org.apache.hadoop.hbase.HBaseParameterizedTestTemplate;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestTemplate;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/JKSFileLoaderTest.java">Base
 *      revision</a>
 */
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
@HBaseParameterizedTestTemplate(name = "{index}: caKeyType={0}, certKeyType={1}, keyPassword={2}")
public class TestJKSFileLoader extends AbstractTestX509Parameterized {

  public TestJKSFileLoader(X509KeyType caKeyType, X509KeyType certKeyType, char[] keyPassword) {
    super(caKeyType, certKeyType, keyPassword);
  }

  @TestTemplate
  public void testLoadKeyStore() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath();
    KeyStore ks = new JKSFileLoader.Builder().setKeyStorePath(path)
      .setKeyStorePassword(x509TestContext.getKeyStorePassword()).build().loadKeyStore();
    assertEquals(1, ks.size());
  }

  @TestTemplate
  public void testLoadKeyStoreWithWrongPassword() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath();
    assertThrows(IOException.class, () -> {
      new JKSFileLoader.Builder().setKeyStorePath(path)
        .setKeyStorePassword("wrong password".toCharArray()).build().loadKeyStore();
    });
  }

  @TestTemplate
  public void testLoadKeyStoreWithWrongFilePath() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.JKS).getAbsolutePath();
    assertThrows(IOException.class, () -> {
      new JKSFileLoader.Builder().setKeyStorePath(path + ".does_not_exist")
        .setKeyStorePassword(x509TestContext.getKeyStorePassword()).build().loadKeyStore();
    });
  }

  @TestTemplate
  public void testLoadKeyStoreWithNullFilePath() {
    assertThrows(NullPointerException.class, () -> {
      new JKSFileLoader.Builder().setKeyStorePassword(x509TestContext.getKeyStorePassword()).build()
        .loadKeyStore();
    });
  }

  @TestTemplate
  public void testLoadKeyStoreWithWrongFileType() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath();
    assertThrows(IOException.class, () -> {
      // Trying to load a PEM file with JKS loader should fail
      new JKSFileLoader.Builder().setKeyStorePath(path)
        .setKeyStorePassword(x509TestContext.getKeyStorePassword()).build().loadKeyStore();
    });
  }

  @TestTemplate
  public void testLoadTrustStore() throws Exception {
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath();
    KeyStore ts = new JKSFileLoader.Builder().setTrustStorePath(path)
      .setTrustStorePassword(x509TestContext.getTrustStorePassword()).build().loadTrustStore();
    assertEquals(1, ts.size());
  }

  @TestTemplate
  public void testLoadTrustStoreWithWrongPassword() throws Exception {
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath();
    assertThrows(IOException.class, () -> {
      new JKSFileLoader.Builder().setTrustStorePath(path)
        .setTrustStorePassword("wrong password".toCharArray()).build().loadTrustStore();
    });
  }

  @TestTemplate
  public void testLoadTrustStoreWithWrongFilePath() throws Exception {
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.JKS).getAbsolutePath();
    assertThrows(IOException.class, () -> {
      new JKSFileLoader.Builder().setTrustStorePath(path + ".does_not_exist")
        .setTrustStorePassword(x509TestContext.getTrustStorePassword()).build().loadTrustStore();
    });
  }

  @TestTemplate
  public void testLoadTrustStoreWithNullFilePath() {
    assertThrows(NullPointerException.class, () -> {
      new JKSFileLoader.Builder().setTrustStorePassword(x509TestContext.getTrustStorePassword())
        .build().loadTrustStore();
    });
  }

  @TestTemplate
  public void testLoadTrustStoreWithWrongFileType() {
    assertThrows(IOException.class, () -> {
      // Trying to load a PEM file with JKS loader should fail
      String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath();
      new JKSFileLoader.Builder().setTrustStorePath(path)
        .setTrustStorePassword(x509TestContext.getTrustStorePassword()).build().loadTrustStore();
    });
  }
}
