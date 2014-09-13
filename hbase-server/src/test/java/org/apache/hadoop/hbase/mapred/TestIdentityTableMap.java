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
package org.apache.hadoop.hbase.mapred;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category({MapReduceTests.class, SmallTests.class})
public class TestIdentityTableMap {

  @Test
  @SuppressWarnings({ "deprecation", "unchecked" })
  public void shouldCollectPredefinedTimes() throws IOException {
    int recordNumber = 999;
    Result resultMock = mock(Result.class);
    IdentityTableMap identityTableMap = null;
    try {
      Reporter reporterMock = mock(Reporter.class);
      identityTableMap = new IdentityTableMap();
      ImmutableBytesWritable bytesWritableMock = mock(ImmutableBytesWritable.class);
      OutputCollector<ImmutableBytesWritable, Result> outputCollectorMock =
          mock(OutputCollector.class);
  
      for (int i = 0; i < recordNumber; i++)
        identityTableMap.map(bytesWritableMock, resultMock, outputCollectorMock,
            reporterMock);
  
      verify(outputCollectorMock, times(recordNumber)).collect(
          Mockito.any(ImmutableBytesWritable.class), Mockito.any(Result.class));
    } finally {
      if (identityTableMap != null)
        identityTableMap.close();
    }
  }
}
