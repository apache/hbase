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
package org.apache.hadoop.hbase.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.CallDroppedException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import org.apache.hbase.thirdparty.com.google.protobuf.ServiceException;

@Tag(SmallTests.TAG)
@Tag(SmallTests.TAG)
public class RpcRetryingCallerImplTest {

  @Test
  public void itTranslatesRemoteExceptionFromServiceException() throws DoNotRetryIOException {
    String message = "CDE for test";
    ServiceException exception = new ServiceException(
      new RemoteWithExtrasException(CallDroppedException.class.getName(), message, false));

    Throwable result = RpcRetryingCallerImpl.translateException(exception);
    assertTrue(result instanceof CallDroppedException, "Expect unwrap CallDroppedException");
    assertEquals(message, result.getMessage());
  }
}
