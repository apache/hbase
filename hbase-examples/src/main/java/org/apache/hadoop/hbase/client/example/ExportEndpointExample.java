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
package org.apache.hadoop.hbase.client.example;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.coprocessor.Export;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * A simple example on how to use {@link org.apache.hadoop.hbase.coprocessor.Export}.
 *
 * <p>
 * For the protocol buffer definition of the ExportService, see the source file located under
 * hbase-endpoint/src/main/protobuf/Export.proto.
 * </p>
 */
@InterfaceAudience.Private
public final class ExportEndpointExample {

  public static void main(String[] args) throws Throwable {
    int rowCount = 100;
    byte[] family = Bytes.toBytes("family");
    Configuration conf = HBaseConfiguration.create();
    TableName tableName = TableName.valueOf("ExportEndpointExample");
    try (Connection con = ConnectionFactory.createConnection(conf);
         Admin admin = con.getAdmin()) {
      TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
              // MUST mount the export endpoint
              .setCoprocessor(Export.class.getName())
              .setColumnFamily(ColumnFamilyDescriptorBuilder.of(family))
              .build();
      admin.createTable(desc);

      List<Put> puts = new ArrayList<>(rowCount);
      for (int row = 0; row != rowCount; ++row) {
        byte[] bs = Bytes.toBytes(row);
        Put put = new Put(bs);
        put.addColumn(family, bs, bs);
        puts.add(put);
      }
      try (Table table = con.getTable(tableName)) {
        table.put(puts);
      }

      Path output = new Path("/tmp/ExportEndpointExample_output");
      Scan scan = new Scan();
      Map<byte[], Export.Response> result = Export.run(conf, tableName, scan, output);
      final long totalOutputRows = result.values().stream().mapToLong(v -> v.getRowCount()).sum();
      final long totalOutputCells = result.values().stream().mapToLong(v -> v.getCellCount()).sum();
      System.out.println("table:" + tableName);
      System.out.println("output:" + output);
      System.out.println("total rows:" + totalOutputRows);
      System.out.println("total cells:" + totalOutputCells);
    }
  }

  private ExportEndpointExample(){}
}
