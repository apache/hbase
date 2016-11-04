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
package org.apache.hadoop.hbase.snapshot;

import java.util.Arrays;
import java.util.Locale;

import org.apache.commons.cli.CommandLine;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.SnapshotDescription;
import org.apache.hadoop.hbase.client.SnapshotType;
import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;

/**
 * This is a command line class that will snapshot a given table.
 */
public class CreateSnapshot extends AbstractHBaseTool {
    private SnapshotType snapshotType = SnapshotType.FLUSH;
    private TableName tableName = null;
    private String snapshotName = null;

    public static void main(String[] args) {
        new CreateSnapshot().doStaticMain(args);
    }

    @Override
    protected void addOptions() {
        this.addRequiredOptWithArg("t", "table", "The name of the table");
        this.addRequiredOptWithArg("n", "name", "The name of the created snapshot");
        this.addOptWithArg("s", "snapshot_type",
                "Snapshot Type. FLUSH is default. Posible values are "
                + Arrays.toString(SnapshotType.values()));
    }

    @Override
    protected void processOptions(CommandLine cmd) {
        this.tableName = TableName.valueOf(cmd.getOptionValue('t'));
        this.snapshotName = cmd.getOptionValue('n');
        String snapshotTypeName = cmd.getOptionValue('s');
        if (snapshotTypeName != null) {
          snapshotTypeName = snapshotTypeName.toUpperCase(Locale.ROOT);
          this.snapshotType = SnapshotType.valueOf(snapshotTypeName);
        }
    }

    @Override
    protected int doWork() throws Exception {
        Connection connection = null;
        Admin admin = null;
        try {
            connection = ConnectionFactory.createConnection(getConf());
            admin = connection.getAdmin();
            admin.snapshot(new SnapshotDescription(snapshotName, tableName, snapshotType));
        } catch (Exception e) {
            System.err.println("failed to take the snapshot: " + e.getMessage());
            return -1;
        } finally {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return 0;
    }

}
