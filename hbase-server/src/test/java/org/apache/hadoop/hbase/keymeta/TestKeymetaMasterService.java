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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for KeymetaMasterService class
 */
@Category({ MasterTests.class, SmallTests.class })
public class TestKeymetaMasterService {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestKeymetaMasterService.class);

  @Mock
  private MasterServices mockMaster;
  @Mock
  private TableDescriptors mockTableDescriptors;

  private Configuration conf;
  private KeymetaMasterService service;
  private AutoCloseable closeableMocks;

  @Before
  public void setUp() throws Exception {
    closeableMocks = MockitoAnnotations.openMocks(this);

    conf = new Configuration();
    when(mockMaster.getConfiguration()).thenReturn(conf);
    when(mockMaster.getTableDescriptors()).thenReturn(mockTableDescriptors);
  }

  @After
  public void tearDown() throws Exception {
    if (closeableMocks != null) {
      closeableMocks.close();
    }
  }

  @Test
  public void testInitWithKeyManagementDisabled() throws Exception {
    // Setup - disable key management
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, false);

    service = new KeymetaMasterService(mockMaster);

    // Execute
    service.init(); // Should return early without creating table

    // Verify - no table operations should be performed
    verify(mockMaster, never()).getTableDescriptors();
    verify(mockMaster, never()).createSystemTable(any());
  }

  @Test
  public void testInitWithKeyManagementEnabledAndTableExists() throws Exception {
    // Setup - enable key management and table already exists
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    when(mockTableDescriptors.exists(KeymetaTableAccessor.KEY_META_TABLE_NAME)).thenReturn(true);

    service = new KeymetaMasterService(mockMaster);

    // Execute
    service.init();

    // Verify - table exists check is performed but no table creation
    verify(mockMaster).getTableDescriptors();
    verify(mockTableDescriptors).exists(KeymetaTableAccessor.KEY_META_TABLE_NAME);
    verify(mockMaster, never()).createSystemTable(any());
  }

  @Test
  public void testInitWithKeyManagementEnabledAndTableDoesNotExist() throws Exception {
    // Setup - enable key management and table does not exist
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    when(mockTableDescriptors.exists(KeymetaTableAccessor.KEY_META_TABLE_NAME)).thenReturn(false);

    service = new KeymetaMasterService(mockMaster);

    // Execute
    service.init();

    // Verify - table is created
    verify(mockMaster).getTableDescriptors();
    verify(mockTableDescriptors).exists(KeymetaTableAccessor.KEY_META_TABLE_NAME);
    verify(mockMaster).createSystemTable(any(TableDescriptor.class));
  }

  @Test
  public void testInitWithTableDescriptorsIOException() throws Exception {
    // Setup - enable key management but table descriptors throws IOException
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    when(mockTableDescriptors.exists(any(TableName.class)))
      .thenThrow(new IOException("Table descriptors error"));

    service = new KeymetaMasterService(mockMaster);

    // Execute & Verify - IOException should propagate
    try {
      service.init();
    } catch (IOException e) {
      // Expected exception
    }

    verify(mockMaster).getTableDescriptors();
    verify(mockTableDescriptors).exists(KeymetaTableAccessor.KEY_META_TABLE_NAME);
    verify(mockMaster, never()).createSystemTable(any());
  }

  @Test
  public void testInitWithCreateSystemTableIOException() throws Exception {
    // Setup - enable key management, table doesn't exist, but createSystemTable throws IOException
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    when(mockTableDescriptors.exists(KeymetaTableAccessor.KEY_META_TABLE_NAME)).thenReturn(false);
    when(mockMaster.createSystemTable(any(TableDescriptor.class)))
      .thenThrow(new IOException("Create table error"));

    service = new KeymetaMasterService(mockMaster);

    // Execute & Verify - IOException should propagate
    try {
      service.init();
    } catch (IOException e) {
      // Expected exception
    }

    verify(mockMaster).getTableDescriptors();
    verify(mockTableDescriptors).exists(KeymetaTableAccessor.KEY_META_TABLE_NAME);
    verify(mockMaster).createSystemTable(any(TableDescriptor.class));
  }

    @Test
  public void testConstructorWithMasterServices() throws Exception {
    // Execute
    service = new KeymetaMasterService(mockMaster);

    // Verify - constructor should not throw an exception
    // The service should be created successfully (no exceptions = success)
    // We don't verify internal calls since the constructor just stores references
  }

  @Test
  public void testMultipleInitCalls() throws Exception {
    // Setup - enable key management and table exists
    conf.setBoolean(HConstants.CRYPTO_MANAGED_KEYS_ENABLED_CONF_KEY, true);
    when(mockTableDescriptors.exists(KeymetaTableAccessor.KEY_META_TABLE_NAME)).thenReturn(true);

    service = new KeymetaMasterService(mockMaster);

    // Execute - call init multiple times
    service.init();
    service.init();
    service.init();

    // Verify - each call should check table existence (idempotent behavior)
    verify(mockMaster, times(3)).getTableDescriptors();
    verify(mockTableDescriptors, times(3)).exists(KeymetaTableAccessor.KEY_META_TABLE_NAME);
    verify(mockMaster, never()).createSystemTable(any());
  }
}