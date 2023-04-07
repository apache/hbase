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

import java.io.IOException;
import java.security.KeyStore;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/BCFKSFileLoaderTest.java">Base
 *      revision</a>
 */
@RunWith(Parameterized.class)
@Category({ SecurityTests.class, SmallTests.class })
public class TestBCFKSFileLoader extends AbstractTestX509Parameterized {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestBCFKSFileLoader.class);

  @Test
  public void testLoadKeyStore() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.BCFKS).getAbsolutePath();
    KeyStore ks = new BCFKSFileLoader.Builder().setKeyStorePath(path)
      .setKeyStorePassword(x509TestContext.getKeyStorePassword()).build().loadKeyStore();
    Assert.assertEquals(1, ks.size());
  }

  @Test(expected = Exception.class)
  public void testLoadKeyStoreWithWrongPassword() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.BCFKS).getAbsolutePath();
    new BCFKSFileLoader.Builder().setKeyStorePath(path)
      .setKeyStorePassword("wrong password".toCharArray()).build().loadKeyStore();
  }

  @Test(expected = IOException.class)
  public void testLoadKeyStoreWithWrongFilePath() throws Exception {
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.BCFKS).getAbsolutePath();
    new BCFKSFileLoader.Builder().setKeyStorePath(path + ".does_not_exist")
      .setKeyStorePassword(x509TestContext.getKeyStorePassword()).build().loadKeyStore();
  }

  @Test(expected = NullPointerException.class)
  public void testLoadKeyStoreWithNullFilePath() throws Exception {
    new BCFKSFileLoader.Builder().setKeyStorePassword(x509TestContext.getKeyStorePassword()).build()
      .loadKeyStore();
  }

  @Test(expected = IOException.class)
  public void testLoadKeyStoreWithWrongFileType() throws Exception {
    // Trying to load a PEM file with BCFKS loader should fail
    String path = x509TestContext.getKeyStoreFile(KeyStoreFileType.PEM).getAbsolutePath();
    new BCFKSFileLoader.Builder().setKeyStorePath(path)
      .setKeyStorePassword(x509TestContext.getKeyStorePassword()).build().loadKeyStore();
  }

  @Test
  public void testLoadTrustStore() throws Exception {
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.BCFKS).getAbsolutePath();
    KeyStore ts = new BCFKSFileLoader.Builder().setTrustStorePath(path)
      .setTrustStorePassword(x509TestContext.getTrustStorePassword()).build().loadTrustStore();
    Assert.assertEquals(1, ts.size());
  }

  @Test(expected = Exception.class)
  public void testLoadTrustStoreWithWrongPassword() throws Exception {
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.BCFKS).getAbsolutePath();
    new BCFKSFileLoader.Builder().setTrustStorePath(path)
      .setTrustStorePassword("wrong password".toCharArray()).build().loadTrustStore();
  }

  @Test(expected = IOException.class)
  public void testLoadTrustStoreWithWrongFilePath() throws Exception {
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.BCFKS).getAbsolutePath();
    new BCFKSFileLoader.Builder().setTrustStorePath(path + ".does_not_exist")
      .setTrustStorePassword(x509TestContext.getTrustStorePassword()).build().loadTrustStore();
  }

  @Test(expected = NullPointerException.class)
  public void testLoadTrustStoreWithNullFilePath() throws Exception {
    new BCFKSFileLoader.Builder().setTrustStorePassword(x509TestContext.getTrustStorePassword())
      .build().loadTrustStore();
  }

  @Test(expected = IOException.class)
  public void testLoadTrustStoreWithWrongFileType() throws Exception {
    // Trying to load a PEM file with BCFKS loader should fail
    String path = x509TestContext.getTrustStoreFile(KeyStoreFileType.PEM).getAbsolutePath();
    new BCFKSFileLoader.Builder().setTrustStorePath(path)
      .setTrustStorePassword(x509TestContext.getTrustStorePassword()).build().loadTrustStore();
  }

}
