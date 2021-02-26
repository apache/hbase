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
package org.apache.hadoop.hbase.errorhandling;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Test that we correctly serialize exceptions from a remote source
 */
@Category({MasterTests.class, SmallTests.class})
public class TestForeignExceptionSerialization {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
      HBaseClassTestRule.forClass(TestForeignExceptionSerialization.class);

  private static final String srcName = "someNode";

  /**
   * Verify that we get back similar stack trace information before an after serialization.
   */
  @Test
  public void testSimpleException() throws IOException {
    String data = "some bytes";
    ForeignException in = new ForeignException("SRC", new IllegalArgumentException(data));
    // check that we get the data back out
    ForeignException e = ForeignException.deserialize(ForeignException.serialize(srcName, in));
    assertNotNull(e);

    // now check that we get the right stack trace
    StackTraceElement elem = new StackTraceElement(this.getClass().toString(), "method", "file", 1);
    in.setStackTrace(new StackTraceElement[] { elem });
    e = ForeignException.deserialize(ForeignException.serialize(srcName, in));

    assertNotNull(e);
    assertEquals("Stack trace got corrupted", elem, e.getCause().getStackTrace()[0]);
    assertEquals("Got an unexpectedly long stack trace", 1, e.getCause().getStackTrace().length);
  }

  /**
   * Compare that a generic exception's stack trace has the same stack trace elements after
   * serialization and deserialization
   */
  @Test
  public void testRemoteFromLocal() throws IOException {
    String errorMsg = "some message";
    Exception generic = new Exception(errorMsg);
    generic.printStackTrace();
    assertTrue(generic.getMessage().contains(errorMsg));

    ForeignException e = ForeignException.deserialize(ForeignException.serialize(srcName, generic));

    // Workaround for java 11 - replaced assertArrayEquals with individual elements comparison
    // using custom comparison helper method
    assertEquals("Stacktrace lengths don't match", generic.getStackTrace().length,
        e.getCause().getStackTrace().length);
    for (int i = 0; i < generic.getStackTrace().length; i++) {
      assertTrue("Local stack trace got corrupted at " + i + "th index",
          compareStackTraceElement(generic.getStackTrace()[i], e.getCause().getStackTrace()[i]));
    }

    e.printStackTrace(); // should have ForeignException and source node in it.
    assertTrue(e.getCause().getCause() == null);

    // verify that original error message is present in Foreign exception message
    assertTrue(e.getCause().getMessage().contains(errorMsg));
  }

  // Helper method to compare two stackTraceElements
  private boolean compareStackTraceElement(StackTraceElement obj1, StackTraceElement obj2) {
    return obj1.getClassName().equals(obj2.getClassName()) && obj1.getLineNumber() == obj2
        .getLineNumber() && Objects.equals(obj1.getMethodName(), obj2.getMethodName()) && Objects
        .equals(obj1.getFileName(), obj2.getFileName());
  }
}
