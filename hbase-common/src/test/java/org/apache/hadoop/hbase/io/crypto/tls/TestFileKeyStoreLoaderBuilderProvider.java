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

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/FileKeyStoreLoaderBuilderProviderTest.java">Base
 *      revision</a>
 */
@Category({ SecurityTests.class, SmallTests.class })
public class TestFileKeyStoreLoaderBuilderProvider {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestFileKeyStoreLoaderBuilderProvider.class);

  @Test
  public void testGetBuilderForJKSFileType() {
    FileKeyStoreLoader.Builder<?> builder =
      FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(KeyStoreFileType.JKS);
    Assert.assertTrue(builder instanceof JKSFileLoader.Builder);
  }

  @Test
  public void testGetBuilderForPEMFileType() {
    FileKeyStoreLoader.Builder<?> builder =
      FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(KeyStoreFileType.PEM);
    Assert.assertTrue(builder instanceof PEMFileLoader.Builder);
  }

  @Test
  public void testGetBuilderForPKCS12FileType() {
    FileKeyStoreLoader.Builder<?> builder =
      FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(KeyStoreFileType.PKCS12);
    Assert.assertTrue(builder instanceof PKCS12FileLoader.Builder);
  }

  @Test(expected = NullPointerException.class)
  public void testGetBuilderForNullFileType() {
    FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(null);
  }
}
