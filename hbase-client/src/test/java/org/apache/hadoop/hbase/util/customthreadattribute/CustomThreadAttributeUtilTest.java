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
import org.slf4j.MDC;

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
