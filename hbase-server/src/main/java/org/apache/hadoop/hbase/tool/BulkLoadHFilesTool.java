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
package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * The implementation for {@link BulkLoadHFiles}, and also can be executed from command line as a
 * tool.
 */
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class BulkLoadHFilesTool extends LoadIncrementalHFiles implements BulkLoadHFiles {

  public static final String NAME = "completebulkload";

  public BulkLoadHFilesTool(Configuration conf) {
    super(conf);
  }

  private Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> convert(
      Map<LoadIncrementalHFiles.LoadQueueItem, ByteBuffer> map) {
    return map.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));
  }

  @Override
  public Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> bulkLoad(TableName tableName,
      Map<byte[], List<Path>> family2Files) throws TableNotFoundException, IOException {
    return convert(run(family2Files, tableName));
  }

  @Override
  public Map<BulkLoadHFiles.LoadQueueItem, ByteBuffer> bulkLoad(TableName tableName, Path dir)
      throws TableNotFoundException, IOException {
    return convert(run(dir, tableName));
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    int ret = ToolRunner.run(conf, new BulkLoadHFilesTool(conf), args);
    System.exit(ret);
  }

}
