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
package org.apache.hadoop.hbase.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.ErasureCodingPolicy;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestErasureCodingUtils {
  private static final String POLICY = "XOR-2-1-1024k";
  private static final Path ROOT_DIR = new Path("/hbase");
  private static final TableName TABLE_NAME = TableName.valueOf("test");
  private static final Path TABLE_DIR = CommonFSUtils.getTableDir(ROOT_DIR, TABLE_NAME);

  @Test
  public void testVerifySupportOnLocalFileSystem() throws IOException {
    Configuration conf = new Configuration();
    FileSystem.setDefaultUri(conf, "file:///");
    ErasureCodingUtils.verifySupport(conf, POLICY);
  }

  @Test
  public void testPolicyManagementOnUnsupportedFileSystem() throws IOException {
    FileSystem fs = mock(LocalFileSystem.class);
    TableDescriptor descriptor =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setErasureCodingPolicy(POLICY).build();
    ErasureCodingUtils.setPolicy(fs, TABLE_DIR, POLICY);
    ErasureCodingUtils.setPolicy(fs, ROOT_DIR, TABLE_NAME, POLICY);
    ErasureCodingUtils.unsetPolicy(fs, ROOT_DIR, TABLE_NAME);
    ErasureCodingUtils.sync(fs, ROOT_DIR, descriptor);
    ErasureCodingUtils.sync(fs, ROOT_DIR,
      TableDescriptorBuilder.newBuilder(descriptor).setErasureCodingPolicy(null).build());
    assertEquals(POLICY, descriptor.getErasureCodingPolicy());
    verifyNoInteractions(fs);
  }

  @Test
  public void testSyncSetsHdfsPolicy() throws IOException {
    DistributedFileSystem dfs = mock(DistributedFileSystem.class);
    TableDescriptor descriptor =
      TableDescriptorBuilder.newBuilder(TABLE_NAME).setErasureCodingPolicy(POLICY).build();
    ErasureCodingUtils.sync(dfs, ROOT_DIR, descriptor);
    verify(dfs).setErasureCodingPolicy(TABLE_DIR, POLICY);
  }

  @Test
  public void testSyncUnsetsHdfsPolicy() throws IOException {
    DistributedFileSystem dfs = mock(DistributedFileSystem.class);
    when(dfs.getErasureCodingPolicy(TABLE_DIR)).thenReturn(mock(ErasureCodingPolicy.class));
    ErasureCodingUtils.sync(dfs, ROOT_DIR, TableDescriptorBuilder.newBuilder(TABLE_NAME).build());
    verify(dfs).unsetErasureCodingPolicy(TABLE_DIR);
  }

  @Test
  public void testUnsetMissingHdfsPolicy() throws IOException {
    DistributedFileSystem dfs = mock(DistributedFileSystem.class);
    ErasureCodingUtils.unsetPolicy(dfs, ROOT_DIR, TABLE_NAME);
    verify(dfs, never()).unsetErasureCodingPolicy(TABLE_DIR);
  }

  @Test
  public void testHdfsErrorsArePropagated() throws IOException {
    DistributedFileSystem dfs = mock(DistributedFileSystem.class);
    IOException failure = new IOException("HDFS unavailable");
    doThrow(failure).when(dfs).setErasureCodingPolicy(TABLE_DIR, POLICY);
    assertSame(failure,
      assertThrows(IOException.class, () -> ErasureCodingUtils.setPolicy(dfs, TABLE_DIR, POLICY)));

    when(dfs.getErasureCodingPolicy(TABLE_DIR)).thenThrow(failure);
    assertSame(failure, assertThrows(IOException.class,
      () -> ErasureCodingUtils.unsetPolicy(dfs, ROOT_DIR, TABLE_NAME)));

    doReturn(mock(ErasureCodingPolicy.class)).when(dfs).getErasureCodingPolicy(TABLE_DIR);
    doThrow(failure).when(dfs).unsetErasureCodingPolicy(TABLE_DIR);
    assertSame(failure, assertThrows(IOException.class,
      () -> ErasureCodingUtils.unsetPolicy(dfs, ROOT_DIR, TABLE_NAME)));
  }
}
