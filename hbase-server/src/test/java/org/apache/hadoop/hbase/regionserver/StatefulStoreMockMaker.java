/**
 *
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
package org.apache.hadoop.hbase.regionserver;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.security.User;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This class is a helper that allows to create a partially-implemented, stateful mocks of
 * Store. It contains a bunch of blank methods, and answers redirecting to these.
 */
public class StatefulStoreMockMaker {
  // Add and expand the methods and answers as needed.
  public CompactionContext selectCompaction() { return null; }
  public void cancelCompaction(Object originalContext) {}
  public int getPriority() { return 0; }

  private class SelectAnswer implements Answer<CompactionContext> {
    public CompactionContext answer(InvocationOnMock invocation) throws Throwable {
      return selectCompaction();
    }
  }
  private class PriorityAnswer implements Answer<Integer> {
    public Integer answer(InvocationOnMock invocation) throws Throwable {
      return getPriority();
    }
  }
  private class CancelAnswer implements Answer<Object> {
    public CompactionContext answer(InvocationOnMock invocation) throws Throwable {
      cancelCompaction(invocation.getArguments()[0]); return null;
    }
  }

  public Store createStoreMock(String name) throws Exception {
    Store store = mock(Store.class, name);
    when(store.requestCompaction(
        anyInt(), isNull(CompactionRequest.class))).then(new SelectAnswer());
    when(store.requestCompaction(
      anyInt(), isNull(CompactionRequest.class), any(User.class))).then(new SelectAnswer());
    when(store.getCompactPriority()).then(new PriorityAnswer());
    doAnswer(new CancelAnswer()).when(
        store).cancelRequestedCompaction(any(CompactionContext.class));
    return store;
  }
}
