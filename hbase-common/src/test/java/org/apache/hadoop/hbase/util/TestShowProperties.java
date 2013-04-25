/**
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
