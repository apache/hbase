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
package org.apache.hadoop.hbase.conf;

import static org.junit.Assert.fail;

import java.util.UUID;
import java.util.function.Consumer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompoundConfiguration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ MiscTests.class, SmallTests.class })
public class TestConfigKey {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestConfigKey.class);

  private interface Interface {
  }

  private class Class implements Interface {
  }

  private void assertThrows(Runnable r) {
    try {
      r.run();
      fail("validation should have failed");
    } catch (IllegalArgumentException e) {
      // Expected
    }
  }

  private void assertPasses(Configuration conf, Consumer<Configuration> block) {
    Configuration copy = new CompoundConfiguration().add(conf);
    block.accept(copy);
    ConfigKey.validate(copy);
  }

  private void assertThrows(Configuration conf, Consumer<Configuration> block) {
    Configuration copy = new CompoundConfiguration().add(conf);
    block.accept(copy);
    assertThrows(() -> ConfigKey.validate(copy));
  }

  @Test
  public void testConfigKey() {
    Configuration conf = new CompoundConfiguration();

    String intKey = UUID.randomUUID().toString();
    ConfigKey.INT(intKey);
    conf.set(intKey, "1");

    String longKey = UUID.randomUUID().toString();
    ConfigKey.LONG(longKey);
    conf.set(longKey, "1");

    String floatKey = UUID.randomUUID().toString();
    ConfigKey.FLOAT(floatKey);
    conf.set(floatKey, "1.0");

    String doubleKey = UUID.randomUUID().toString();
    ConfigKey.DOUBLE(doubleKey);
    conf.set(doubleKey, "1.0");

    String classKey = UUID.randomUUID().toString();
    ConfigKey.CLASS(classKey, Interface.class);
    conf.set(classKey, Class.class.getName());

    String booleanKey = UUID.randomUUID().toString();
    ConfigKey.BOOLEAN(booleanKey);
    conf.set(booleanKey, "true");

    // This should pass
    ConfigKey.validate(conf);

    // Add a predicate to make the validation fail
    ConfigKey.INT(intKey, i -> i < 0);
    assertThrows(() -> ConfigKey.validate(conf));

    // New predicates to make the validation pass
    ConfigKey.INT(intKey, i -> i > 0, i -> i < 2);
    ConfigKey.validate(conf);

    // Remove the predicate
    ConfigKey.INT(intKey);

    // Passing examples
    assertPasses(conf, copy -> copy.set(intKey, String.valueOf(Integer.MAX_VALUE)));
    assertPasses(conf, copy -> copy.set(longKey, String.valueOf(Long.MAX_VALUE)));
    assertPasses(conf, copy -> copy.set(floatKey, String.valueOf(Float.MAX_VALUE)));
    assertPasses(conf, copy -> copy.set(doubleKey, String.valueOf(Double.MAX_VALUE)));

    // Because Configuration#getBoolean doesn't throw an exception on invalid values, we don't
    // validate the value here
    assertPasses(conf, copy -> copy.set(booleanKey, "yeah?"));

    // Failing examples
    assertThrows(conf, copy -> copy.set(intKey, "x"));
    assertThrows(conf, copy -> copy.set(longKey, Long.MAX_VALUE + "0"));
    assertThrows(conf, copy -> copy.set(longKey, "x"));
    assertThrows(conf, copy -> copy.set(floatKey, "x"));
    assertThrows(conf, copy -> copy.set(doubleKey, "x"));
    assertThrows(conf, copy -> copy.set(classKey, "NoSuchClass"));
    assertThrows(conf, copy -> copy.set(classKey, getClass().getName()));
  }
}
