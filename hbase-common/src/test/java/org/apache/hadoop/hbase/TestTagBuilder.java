/**
 * Copyright The Apache Software Foundation
 *
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
package org.apache.hadoop.hbase;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MiscTests.class, SmallTests.class})
public class TestTagBuilder {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTagBuilder.class);

  @Test
  public void testArrayBackedTagBuilder() {
    byte type = (byte)50;
    String value = "Array-Backed-Tag";
    TagBuilder builder = TagBuilderFactory.create();
    assertTrue(builder instanceof TagBuilderImpl);
    builder.setTagType(type);
    builder.setTagValue(Bytes.toBytes(value));
    Tag tag = builder.build();
    assertEquals(value, Tag.getValueAsString(tag));
    assertEquals(type, tag.getType());
  }

  @Test
  public void testErrorMessages() {
    String arrayValue = "Array-Backed-Tag";
    TagBuilder builder = TagBuilderFactory.create();
    builder.setTagValue(Bytes.toBytes(arrayValue));
    try {
      // Dont set type for the tag.
      builder.build();
      fail("Shouldn't have come here.");
    } catch(IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains(TagBuilderImpl.TAG_TYPE_NOT_SET_EXCEPTION));
    }

    byte type = (byte)50;
    builder = TagBuilderFactory.create();
    builder.setTagType(type);
    try {
      // Need to Call setTagValue(byte[]) to set the value.
      builder.build();
      fail("Shouldn't have come here.");
    } catch(IllegalArgumentException iae) {
      assertTrue(iae.getMessage().contains(TagBuilderImpl.TAG_VALUE_NULL_EXCEPTION));
    }
  }
}
