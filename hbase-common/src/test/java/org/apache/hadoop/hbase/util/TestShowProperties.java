package org.apache.hadoop.hbase.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Properties;


/**
 * This test is there to dump the properties. It allows to detect possible env issues when
 * executing the tests on various environment.
 */
@Category(SmallTests.class)
public class TestShowProperties {
  private static final Log LOG = LogFactory.getLog(TestShowProperties.class);

  @Test
  public void testShowProperty() {
    Properties properties = System.getProperties();
    for (java.util.Map.Entry<Object, Object> prop : properties.entrySet()) {
      LOG.info("Property " + prop.getKey() + "=" + prop.getValue());
    }
  }
}
