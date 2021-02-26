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
package org.apache.hadoop.hbase.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({SmallTests.class})
public class TestProfileOutputServlet {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestProfileOutputServlet.class);

  @Test
  public void testSanitization() {
    List<String> good = Arrays.asList("abcd", "key=value", "key1=value&key2=value2", "",
        "host=host-1.example.com");
    for (String input : good) {
      assertEquals(input, ProfileOutputServlet.sanitize(input));
    }
    List<String> bad = Arrays.asList("function(){console.log(\"oops\")}", "<strong>uhoh</strong>");
    for (String input : bad) {
      try {
        ProfileOutputServlet.sanitize(input);
        fail("Expected sanitization of \"" + input + "\" to fail");
      } catch (RuntimeException e) {
        // Pass
      }
    }
  }

}
