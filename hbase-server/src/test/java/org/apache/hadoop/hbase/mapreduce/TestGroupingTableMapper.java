/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may
 * obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hbase.mapreduce;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.testclassification.MapReduceTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.mockito.Mockito.*;

@Category({MapReduceTests.class, SmallTests.class})
public class TestGroupingTableMapper {

  /**
   * Test GroupingTableMapper class
   */
  @Test
  public void testGroupingTableMapper() throws Exception {

    GroupingTableMapper mapper = new GroupingTableMapper();
    Configuration configuration = new Configuration();
    configuration.set(GroupingTableMapper.GROUP_COLUMNS, "family1:clm family2:clm");
    mapper.setConf(configuration);

    Result result = mock(Result.class);
    @SuppressWarnings("unchecked")
    Mapper<ImmutableBytesWritable, Result, ImmutableBytesWritable, Result>.Context context =
        mock(Mapper.Context.class);
    context.write(any(ImmutableBytesWritable.class), any(Result.class));
    List<Cell> keyValue = new ArrayList<Cell>();
    byte[] row = {};
    keyValue.add(new KeyValue(row, Bytes.toBytes("family2"), Bytes.toBytes("clm"), Bytes
        .toBytes("value1")));
    keyValue.add(new KeyValue(row, Bytes.toBytes("family1"), Bytes.toBytes("clm"), Bytes
        .toBytes("value2")));
    when(result.listCells()).thenReturn(keyValue);
    mapper.map(null, result, context);
    // template data
    byte[][] data = { Bytes.toBytes("value1"), Bytes.toBytes("value2") };
    ImmutableBytesWritable ibw = mapper.createGroupKey(data);
    verify(context).write(ibw, result);
  }

}
