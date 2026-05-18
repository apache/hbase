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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.hadoop.hbase.testclassification.SecurityTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * This file has been copied from the Apache ZooKeeper project.
 * @see <a href=
 *      "https://github.com/apache/zookeeper/blob/master/zookeeper-server/src/test/java/org/apache/zookeeper/common/FileKeyStoreLoaderBuilderProviderTest.java">Base
 *      revision</a>
 */
@Tag(SecurityTests.TAG)
@Tag(SmallTests.TAG)
public class TestFileKeyStoreLoaderBuilderProvider {

  @Test
  public void testGetBuilderForJKSFileType() {
    FileKeyStoreLoader.Builder<?> builder =
      FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(KeyStoreFileType.JKS);
    assertThat(builder, instanceOf(JKSFileLoader.Builder.class));
  }

  @Test
  public void testGetBuilderForPEMFileType() {
    FileKeyStoreLoader.Builder<?> builder =
      FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(KeyStoreFileType.PEM);
    assertThat(builder, instanceOf(PEMFileLoader.Builder.class));
  }

  @Test
  public void testGetBuilderForPKCS12FileType() {
    FileKeyStoreLoader.Builder<?> builder =
      FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(KeyStoreFileType.PKCS12);
    assertThat(builder, instanceOf(PKCS12FileLoader.Builder.class));
  }

  @Test
  public void testGetBuilderForNullFileType() {
    assertThrows(NullPointerException.class,
      () -> FileKeyStoreLoaderBuilderProvider.getBuilderForKeyStoreFileType(null));
  }
}
