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
package org.apache.hadoop.hbase.procedure2;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.hadoop.hbase.master.procedure.MasterProcedureEnv;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(SmallTests.TAG)
@Tag(MasterTests.TAG)
public class TestProcedureFutureUtil {

  @Test
  public void testCompleteInSameThread() throws IOException {
    Procedure<?> proc = mock(Procedure.class);
    MutableObject<CompletableFuture<Void>> setFuture = new MutableObject<>();

    CompletableFuture<Void> future = new CompletableFuture<>();

    MasterProcedureEnv env = mock(MasterProcedureEnv.class);
    ExecutorService exec = mock(ExecutorService.class);
    when(env.getAsyncTaskExecutor()).thenReturn(exec);

    Runnable actionAfterDone = mock(Runnable.class);

    assertThrows(ProcedureSuspendedException.class, () -> ProcedureFutureUtil
      .suspendIfNecessary(proc, setFuture::setValue, future, env, actionAfterDone));

    assertSame(future, setFuture.get());
    verify(proc, times(1)).skipPersistence();

    // complete in the same thread, and make sure the callback has been scheduled correctly
    future.complete(null);
    verify(env, times(1)).getAsyncTaskExecutor();
    verify(exec, times(1)).execute(any());
  }
}
