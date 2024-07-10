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
package org.apache.hadoop.hbase.filter;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;
import org.apache.hadoop.hbase.DoNotRetryUncheckedIOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.IncrementingEnvironmentEdge;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(SmallTests.class)
public class RegexStringComparatorTest {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(RegexStringComparatorTest.class);

  @Test(expected = DoNotRetryUncheckedIOException.class)
  public void testCompareToTimeout() {
    final RegexStringComparator.JavaRegexEngine regex =
      new RegexStringComparator.JavaRegexEngine("(0*)*A", Pattern.DOTALL, 0);

    EnvironmentEdgeManager.injectEdge(new IncrementingEnvironmentEdge());
    final byte[] input = "00000000000000000000000000000".getBytes(StandardCharsets.UTF_8);
    // It actually takes a few seconds
    regex.compareTo(input, 0, input.length);
  }
}
