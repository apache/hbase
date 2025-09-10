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
package org.apache.hadoop.hbase.keymeta;


import static org.apache.hadoop.hbase.HConstants.SYSTEM_KEY_FILE_PREFIX;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.io.crypto.MockManagedKeyProvider;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.crypto.KeymetaTestUtils;
import org.apache.hadoop.hbase.io.crypto.ManagedKeyData;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({ MiscTests.class, SmallTests.class })
public class TestKeyManagementService {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeyManagementService.class);

  @Rule
  public TestName name = new TestName();

  protected Configuration conf = new Configuration();
  protected FileSystem mockFileSystem = mock(FileSystem.class);

  @Before
  public void setUp() throws Exception {
    conf.set(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, "true");
    conf.set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY, MockManagedKeyProvider.class.getName());
    conf.set(HConstants.HBASE_ORIGINAL_DIR, "/tmp/hbase");
  }

  @Test
  public void testDefaultKeyManagementServiceCreation() throws IOException {
    // SystemKeyCache needs at least one valid key to be created, so setting up a mock FS that
    // returns a mock file that returns a known mocked key metadata.
    MockManagedKeyProvider provider = (MockManagedKeyProvider) Encryption.getKeyProvider(conf);
    ManagedKeyData keyData = provider.getManagedKey("system".getBytes(),
      ManagedKeyData.KEY_SPACE_GLOBAL);
    String fileName = SYSTEM_KEY_FILE_PREFIX + "1";
    Path systemKeyDir = CommonFSUtils.getSystemKeyDir(conf);
    FileStatus mockFileStatus = KeymetaTestUtils.createMockFile(fileName);
    FSDataInputStream mockStream = mock(FSDataInputStream.class);
    when(mockStream.readUTF()).thenReturn(keyData.getKeyMetadata());
    when(mockFileSystem.open(eq(mockFileStatus.getPath()))).thenReturn(mockStream);
    when(mockFileSystem.globStatus(eq(new Path(systemKeyDir, SYSTEM_KEY_FILE_PREFIX+"*"))))
      .thenReturn(new FileStatus[] { mockFileStatus });

    KeyManagementService service = KeyManagementService.createDefault(conf, mockFileSystem);
    assertNotNull(service);
    assertNotNull(service.getSystemKeyCache());
    assertNotNull(service.getManagedKeyDataCache());
    assertThrows(UnsupportedOperationException.class, () -> service.getKeymetaAdmin());
  }
}