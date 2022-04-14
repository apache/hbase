/**
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
package org.apache.hadoop.hbase.util.customthreadattribute;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class) public class CustomThreadAttributeUtilTest {

  static String KEY = "key";
  static String VALUE = "value";

  @Test
  public void testCustomThreadAttributeUtil() {
    // Initialize the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.CUSTOM_THREAD_ATTRIBUTE_REQUEST_ID_CONTEXT_PREFIX
      + HConstants.CUSTOM_THREAD_ATTRIBUTE_ENABLED_SUFFIX, true);
    conf.set(HConstants.CUSTOM_THREAD_ATTRIBUTE_REQUEST_ID_CONTEXT_PREFIX
        + HConstants.CUSTOM_THREAD_ATTRIBUTE_IMPLEMENTATION_SUFFIX,
      "org.apache.hadoop.hbase.util.customthreadattribute.RequestIdContextHandler");

    // Create the attribute
    CustomThreadAttribute attribute =
      new CustomThreadAttribute(KEY, VALUE, AttributeType.REQUEST_ID_CONTEXT);
    List<CustomThreadAttribute> attributes = new ArrayList<>();
    attributes.add(attribute);

    // Set the attribute
    CustomThreadAttributeUtil.setAttributes(attributes, conf);

    // Assert that we get what we set
    Assert.assertEquals(VALUE, CustomThreadAttributeUtil.getAttribute(attribute, conf).getValue());
    Assert.assertEquals(VALUE, CustomThreadAttributeUtil.getAllAttributes(conf).get(0).getValue());

    // Assert that the value is no longer set
    CustomThreadAttributeUtil.clearAttributes(attributes, conf);
    Assert.assertNull(CustomThreadAttributeUtil.getAttribute(attribute, conf).getValue());
  }

  @Test
  public void testCustomThreadAttributeUtilException(){
    // Initialize the configuration
    Configuration conf = HBaseConfiguration.create();
    conf.setBoolean(HConstants.CUSTOM_THREAD_ATTRIBUTE_REQUEST_ID_CONTEXT_PREFIX
      + HConstants.CUSTOM_THREAD_ATTRIBUTE_ENABLED_SUFFIX, true);
    conf.set(HConstants.CUSTOM_THREAD_ATTRIBUTE_REQUEST_ID_CONTEXT_PREFIX
        + HConstants.CUSTOM_THREAD_ATTRIBUTE_IMPLEMENTATION_SUFFIX,
      "non.existing.class");

    // Create the attribute
    CustomThreadAttribute attribute =
      new CustomThreadAttribute(KEY, VALUE, AttributeType.REQUEST_ID_CONTEXT);
    List<CustomThreadAttribute> attributes = new ArrayList<>();
    attributes.add(attribute);

    // Set the attribute, doesn't throw any exception
    CustomThreadAttributeUtil.setAttributes(attributes, conf);

    // Assert the gets
    Assert.assertNull(CustomThreadAttributeUtil.getAttribute(attribute, conf));
    Assert.assertTrue(CustomThreadAttributeUtil.getAllAttributes(conf).isEmpty());

    // Clear the attribute, doesn't throw any exception
    CustomThreadAttributeUtil.clearAttributes(attributes, conf);
  }
}
