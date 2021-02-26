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

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.hadoop.hbase.regionserver.compactions.CompactionContext;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

/**
 * This class is a helper that allows to create a partially-implemented, stateful mocks of
 * Store. It contains a bunch of blank methods, and answers redirecting to these.
 */
public class StatefulStoreMockMaker {
  // Add and expand the methods and answers as needed.
  public Optional<CompactionContext> selectCompaction() {
    return Optional.empty();
  }

  public void cancelCompaction(Object originalContext) {}

  public int getPriority() {
    return 0;
  }
  private class CancelAnswer implements Answer<Object> {
    @Override
    public CompactionContext answer(InvocationOnMock invocation) throws Throwable {
      cancelCompaction(invocation.getArgument(0));
      return null;
    }
  }

  public HStore createStoreMock(String name) throws Exception {
    HStore store = mock(HStore.class, name);
    when(store.requestCompaction(anyInt(), any(), any())).then(inv -> selectCompaction());
    when(store.getCompactPriority()).then(inv -> getPriority());
    doAnswer(new CancelAnswer()).when(store).cancelRequestedCompaction(any());
    return store;
  }
}
