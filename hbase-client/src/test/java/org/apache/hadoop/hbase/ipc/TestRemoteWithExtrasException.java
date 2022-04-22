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
package org.apache.hadoop.hbase.ipc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.IOException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseServerException;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({ ClientTests.class, SmallTests.class })
public class TestRemoteWithExtrasException {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestRemoteWithExtrasException.class);

  /**
   * test verifies that we honor the inherent value of an exception for isServerOverloaded.
   * We don't want a false value passed into RemoteWithExtrasExceptions to override the
   * inherent value of an exception if it's already true. This could be due to an out of date
   * server not sending the proto field we expect.
   */
  @Test
  public void itUsesExceptionDefaultValueForServerOverloaded() {
    // pass false for server overloaded, we still expect the exception to be true due to
    // the exception type
    RemoteWithExtrasException ex =
      new RemoteWithExtrasException(ServerOverloadedException.class.getName(),
        "server is overloaded", false, false);
    IOException result = ex.unwrapRemoteException();

    assertEquals(result.getClass(), ServerOverloadedException.class);
    assertTrue(((ServerOverloadedException) result).isServerOverloaded());
  }

  @Test
  public void itUsesPassedServerOverloadedValue() {
    String exceptionClass = HBaseServerException.class.getName();
    String message = "server is overloaded";
    RemoteWithExtrasException ex =
      new RemoteWithExtrasException(exceptionClass, message, false, false);
    IOException result = ex.unwrapRemoteException();

    assertTrue(result instanceof HBaseServerException);
    assertFalse(((HBaseServerException) result).isServerOverloaded());

    // run again with true value passed in
    ex = new RemoteWithExtrasException(exceptionClass, message, false, true);
    result = ex.unwrapRemoteException();

    assertTrue(result instanceof HBaseServerException);
    // expect true this time
    assertTrue(((HBaseServerException) result).isServerOverloaded());
  }

  private static class ServerOverloadedException extends HBaseServerException {
    public ServerOverloadedException(String message) {
      super(true, message);
    }
  }

}
