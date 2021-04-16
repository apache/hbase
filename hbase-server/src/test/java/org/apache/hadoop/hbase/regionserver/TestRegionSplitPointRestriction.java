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
package org.apache.hadoop.hbase.regionserver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@Category({ RegionServerTests.class, SmallTests.class })
public class TestRegionSplitPointRestriction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionSplitPointRestriction.class);

  Configuration conf;
  @Mock TableDescriptor tableDescriptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    conf = new Configuration();
  }

  @Test
  public void testWhenTableDescriptorReturnsNoneType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitPointRestriction.RESTRICTION_TYPE_NONE);

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof NoRegionSplitPointRestriction);
  }

  @Test
  public void testWhenTableDescriptorReturnsKeyPrefixType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY_PREFIX);

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof KeyPrefixRegionSplitPointRestriction);
  }

  @Test
  public void testWhenTableDescriptorReturnsDelimitedKeyPrefixType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitPointRestriction.RESTRICTION_TYPE_DELIMITED_KEY_PREFIX);

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof DelimitedKeyPrefixRegionSplitPointRestriction);
  }

  @Test
  public void testWhenConfigurationReturnsNoneType() throws IOException {
    conf.set(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY,
      RegionSplitPointRestriction.RESTRICTION_TYPE_NONE);

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof NoRegionSplitPointRestriction);
  }

  @Test
  public void testWhenConfigurationReturnsKeyPrefixType() throws IOException {
    conf.set(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY,
      RegionSplitPointRestriction.RESTRICTION_TYPE_KEY_PREFIX);

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof KeyPrefixRegionSplitPointRestriction);
  }

  @Test
  public void testWhenConfigurationReturnsDelimitedKeyPrefixType() throws IOException {
    conf.set(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY,
      RegionSplitPointRestriction.RESTRICTION_TYPE_DELIMITED_KEY_PREFIX);

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof DelimitedKeyPrefixRegionSplitPointRestriction);
  }

  @Test
  public void testWhenTableDescriptorAndConfigurationReturnNull() throws IOException {
    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof NoRegionSplitPointRestriction);
  }

  @Test
  public void testWhenTableDescriptorReturnsInvalidType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn("Invalid");

    RegionSplitPointRestriction splitPointRestriction =
      RegionSplitPointRestriction.create(tableDescriptor, conf);
    assertTrue(splitPointRestriction instanceof NoRegionSplitPointRestriction);
  }

  @Test
  public void testNoneRegionSplitPointRestriction() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitPointRestriction.RESTRICTION_TYPE_NONE);

    NoRegionSplitPointRestriction noRegionSplitPointRestriction =
      (NoRegionSplitPointRestriction) RegionSplitPointRestriction.create(tableDescriptor, conf);

    byte[] restrictedSplitPoint =
      noRegionSplitPointRestriction.getRestrictedSplitPoint(Bytes.toBytes("abcd"));
    assertEquals("abcd", Bytes.toString(restrictedSplitPoint));
  }

  @Test
  public void testKeyPrefixRegionSplitPointRestriction() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY_PREFIX);
    when(tableDescriptor.getValue(KeyPrefixRegionSplitPointRestriction.PREFIX_LENGTH_KEY))
      .thenReturn("2");

    KeyPrefixRegionSplitPointRestriction keyPrefixRegionSplitPointRestriction =
      (KeyPrefixRegionSplitPointRestriction) RegionSplitPointRestriction.create(
        tableDescriptor, conf);

    byte[] restrictedSplitPoint =
      keyPrefixRegionSplitPointRestriction.getRestrictedSplitPoint(Bytes.toBytes("abcd"));
    assertEquals("ab", Bytes.toString(restrictedSplitPoint));

    restrictedSplitPoint =
      keyPrefixRegionSplitPointRestriction.getRestrictedSplitPoint(Bytes.toBytes("a"));
    assertEquals("a", Bytes.toString(restrictedSplitPoint));
  }

  @Test
  public void testDelimitedKeyPrefixRegionSplitPointRestriction() throws IOException {
    when(tableDescriptor.getValue(RegionSplitPointRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitPointRestriction.RESTRICTION_TYPE_DELIMITED_KEY_PREFIX);
    when(tableDescriptor.getValue(DelimitedKeyPrefixRegionSplitPointRestriction.DELIMITER_KEY))
      .thenReturn(",");

    DelimitedKeyPrefixRegionSplitPointRestriction delimitedKeyPrefixRegionSplitPointRestriction =
      (DelimitedKeyPrefixRegionSplitPointRestriction) RegionSplitPointRestriction.create(
        tableDescriptor, conf);

    byte[] restrictedSplitPoint = delimitedKeyPrefixRegionSplitPointRestriction
      .getRestrictedSplitPoint(Bytes.toBytes("ab,cd"));
    assertEquals("ab", Bytes.toString(restrictedSplitPoint));

    restrictedSplitPoint = delimitedKeyPrefixRegionSplitPointRestriction
      .getRestrictedSplitPoint(Bytes.toBytes("ijk"));
    assertEquals("ijk", Bytes.toString(restrictedSplitPoint));
  }
}
