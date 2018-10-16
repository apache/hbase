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

import static org.junit.Assert.assertEquals;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

@Category(SmallTests.class)
public class TestJRubyFormat {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestJRubyFormat.class);

  @Test
  public void testPrint() {
    Map<String, Object> map = new LinkedHashMap<>();
    map.put("null", null);
    map.put("boolean", true);
    map.put("number", 1);
    map.put("string", "str");
    map.put("binary", new byte[] { 1, 2, 3 });
    map.put("list", Lists.newArrayList(1, "2", true));

    String jrubyString = JRubyFormat.print(map);
    assertEquals("{ null => '', boolean => 'true', number => '1', "
        + "string => 'str', binary => '010203', "
        + "list => [ '1', '2', 'true' ] }", jrubyString);
  }

  @Test
  public void testEscape() {
    String jrubyString = JRubyFormat.print("\\\'\n\r\t\f");
    assertEquals("'\\\\\\'\\n\\r\\t\\f'", jrubyString);
  }
}
