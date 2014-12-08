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

package org.apache.hadoop.hbase;

import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Checks tests are categorized.
 */
@Category(SmallTests.class)
public class TestCheckTestClasses {
  /**
   * Throws an assertion if we find a test class without category (small/medium/large/integration).
   * List all the test classes without category in the assertion message.
   */
  @Test
  public void checkClasses() throws Exception {
    List<Class<?>> badClasses = new java.util.ArrayList<Class<?>>();
    ClassTestFinder classFinder = new ClassTestFinder();
    for (Class<?> c : classFinder.findClasses(false)) {
      if (ClassTestFinder.getCategoryAnnotations(c).length == 0) {
        badClasses.add(c);
      }
    }
    assertTrue("There are " + badClasses.size() + " test classes without category: "
      + badClasses, badClasses.isEmpty());
  }
}
