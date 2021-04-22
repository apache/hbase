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
public class TestRegionSplitRestriction {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRegionSplitRestriction.class);

  Configuration conf;
  @Mock TableDescriptor tableDescriptor;

  @Before
  public void setup() {
    MockitoAnnotations.initMocks(this);

    conf = new Configuration();
  }

  @Test
  public void testWhenTableDescriptorReturnsNoneType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitRestriction.RESTRICTION_TYPE_NONE);

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof NoRegionSplitRestriction);
  }

  @Test
  public void testWhenTableDescriptorReturnsKeyPrefixType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitRestriction.RESTRICTION_TYPE_KEY_PREFIX);

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof KeyPrefixRegionSplitRestriction);
  }

  @Test
  public void testWhenTableDescriptorReturnsDelimitedKeyPrefixType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitRestriction.RESTRICTION_TYPE_DELIMITED_KEY_PREFIX);

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof DelimitedKeyPrefixRegionSplitRestriction);
  }

  @Test
  public void testWhenConfigurationReturnsNoneType() throws IOException {
    conf.set(RegionSplitRestriction.RESTRICTION_TYPE_KEY,
      RegionSplitRestriction.RESTRICTION_TYPE_NONE);

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof NoRegionSplitRestriction);
  }

  @Test
  public void testWhenConfigurationReturnsKeyPrefixType() throws IOException {
    conf.set(RegionSplitRestriction.RESTRICTION_TYPE_KEY,
      RegionSplitRestriction.RESTRICTION_TYPE_KEY_PREFIX);

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof KeyPrefixRegionSplitRestriction);
  }

  @Test
  public void testWhenConfigurationReturnsDelimitedKeyPrefixType() throws IOException {
    conf.set(RegionSplitRestriction.RESTRICTION_TYPE_KEY,
      RegionSplitRestriction.RESTRICTION_TYPE_DELIMITED_KEY_PREFIX);

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof DelimitedKeyPrefixRegionSplitRestriction);
  }

  @Test
  public void testWhenTableDescriptorAndConfigurationReturnNull() throws IOException {
    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof NoRegionSplitRestriction);
  }

  @Test
  public void testWhenTableDescriptorReturnsInvalidType() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn("Invalid");

    RegionSplitRestriction splitRestriction =
      RegionSplitRestriction.create(tableDescriptor, conf);
    assertTrue(splitRestriction instanceof NoRegionSplitRestriction);
  }

  @Test
  public void testNoneRegionSplitRestriction() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitRestriction.RESTRICTION_TYPE_NONE);

    NoRegionSplitRestriction noRegionSplitRestriction =
      (NoRegionSplitRestriction) RegionSplitRestriction.create(tableDescriptor, conf);

    byte[] restrictedSplit =
      noRegionSplitRestriction.getRestrictedSplitPoint(Bytes.toBytes("abcd"));
    assertEquals("abcd", Bytes.toString(restrictedSplit));
  }

  @Test
  public void testKeyPrefixRegionSplitRestriction() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitRestriction.RESTRICTION_TYPE_KEY_PREFIX);
    when(tableDescriptor.getValue(KeyPrefixRegionSplitRestriction.PREFIX_LENGTH_KEY))
      .thenReturn("2");

    KeyPrefixRegionSplitRestriction keyPrefixRegionSplitRestriction =
      (KeyPrefixRegionSplitRestriction) RegionSplitRestriction.create(
        tableDescriptor, conf);

    byte[] restrictedSplit =
      keyPrefixRegionSplitRestriction.getRestrictedSplitPoint(Bytes.toBytes("abcd"));
    assertEquals("ab", Bytes.toString(restrictedSplit));

    restrictedSplit =
      keyPrefixRegionSplitRestriction.getRestrictedSplitPoint(Bytes.toBytes("a"));
    assertEquals("a", Bytes.toString(restrictedSplit));
  }

  @Test
  public void testDelimitedKeyPrefixRegionSplitRestriction() throws IOException {
    when(tableDescriptor.getValue(RegionSplitRestriction.RESTRICTION_TYPE_KEY))
      .thenReturn(RegionSplitRestriction.RESTRICTION_TYPE_DELIMITED_KEY_PREFIX);
    when(tableDescriptor.getValue(DelimitedKeyPrefixRegionSplitRestriction.DELIMITER_KEY))
      .thenReturn(",");

    DelimitedKeyPrefixRegionSplitRestriction delimitedKeyPrefixRegionSplitRestriction =
      (DelimitedKeyPrefixRegionSplitRestriction) RegionSplitRestriction.create(
        tableDescriptor, conf);

    byte[] restrictedSplit = delimitedKeyPrefixRegionSplitRestriction
      .getRestrictedSplitPoint(Bytes.toBytes("ab,cd"));
    assertEquals("ab", Bytes.toString(restrictedSplit));

    restrictedSplit = delimitedKeyPrefixRegionSplitRestriction
      .getRestrictedSplitPoint(Bytes.toBytes("ijk"));
    assertEquals("ijk", Bytes.toString(restrictedSplit));
  }
}
