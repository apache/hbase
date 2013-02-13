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
package org.apache.hadoop.hbase.server.snapshot.task;

import org.apache.hadoop.hbase.SmallTests;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.SnapshotDescription;
import org.apache.hadoop.hbase.server.snapshot.error.SnapshotExceptionSnare;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(SmallTests.class)
public class TestSnapshotTask {

  /**
   * Check that errors from running the task get propagated back to the error listener.
   */
  @Test
  public void testErrorPropagationg() {
    SnapshotExceptionSnare error = Mockito.mock(SnapshotExceptionSnare.class);
    SnapshotDescription snapshot = SnapshotDescription.newBuilder().setName("snapshot")
        .setTable("table").build();
    final Exception thrown = new Exception("Failed!");
    SnapshotTask fail = new SnapshotTask(snapshot, error, "always fails") {

      @Override
      protected void process() throws Exception {
        throw thrown;
      }
    };
    fail.run();

    Mockito.verify(error, Mockito.times(1)).snapshotFailure(Mockito.anyString(),
      Mockito.eq(snapshot), Mockito.eq(thrown));
  }

}
