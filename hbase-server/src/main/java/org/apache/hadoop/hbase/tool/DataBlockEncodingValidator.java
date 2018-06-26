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
package org.apache.hadoop.hbase.tool;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.org.apache.commons.cli.CommandLine;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
public class DataBlockEncodingValidator extends AbstractHBaseTool {

  private static final Logger LOG = LoggerFactory.getLogger(DataBlockEncodingValidator.class);
  private static final byte[] DATA_BLOCK_ENCODING = Bytes.toBytes("DATA_BLOCK_ENCODING");

  /**
   * Check DataBlockEncodings of column families are compatible.
   *
   * @return number of column families with incompatible DataBlockEncoding
   * @throws IOException if a remote or network exception occurs
   */
  private int validateDBE() throws IOException {
    int incompatibilities = 0;

    LOG.info("Validating Data Block Encodings");

    try (Connection connection = ConnectionFactory.createConnection(getConf());
        Admin admin = connection.getAdmin()) {
      List<TableDescriptor> tableDescriptors = admin.listTableDescriptors();
      String encoding = "";

      for (TableDescriptor td : tableDescriptors) {
        ColumnFamilyDescriptor[] columnFamilies = td.getColumnFamilies();
        for (ColumnFamilyDescriptor cfd : columnFamilies) {
          try {
            encoding = Bytes.toString(cfd.getValue(DATA_BLOCK_ENCODING));
            // IllegalArgumentException will be thrown if encoding is incompatible with 2.0
            DataBlockEncoding.valueOf(encoding);
          } catch (IllegalArgumentException e) {
            incompatibilities++;
            LOG.warn("Incompatible DataBlockEncoding for table: {}, cf: {}, encoding: {}",
                td.getTableName().getNameAsString(), cfd.getNameAsString(), encoding);
          }
        }
      }
    }

    if (incompatibilities > 0) {
      LOG.warn("There are {} column families with incompatible Data Block Encodings. Do not "
          + "upgrade until these encodings are converted to a supported one. "
          + "Check https://s.apache.org/prefixtree for instructions.", incompatibilities);
    } else {
      LOG.info("The used Data Block Encodings are compatible with HBase 2.0.");
    }

    return incompatibilities;
  }

  @Override
  protected void printUsage() {
    String header = "hbase " + PreUpgradeValidator.TOOL_NAME + " " +
        PreUpgradeValidator.VALIDATE_DBE_NAME;
    printUsage(header, null, "");
  }

  @Override
  protected void addOptions() {
  }

  @Override
  protected void processOptions(CommandLine cmd) {
  }

  @Override
  protected int doWork() throws Exception {
    return (validateDBE() == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
  }
}
