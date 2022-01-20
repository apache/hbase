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
package org.apache.hadoop.hbase.jwt.client.example;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.hadoop.hbase.security.token.OAuthBearerTokenUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example of using OAuthBearer (JWT) authentication with HBase RPC client.
 */
@InterfaceAudience.Private
public class JwtClientExample extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(JwtClientExample.class);
  private static final String JWT_TOKEN = "<base64_encoded_jwt_token>";

  private static final byte[] FAMILY = Bytes.toBytes("d");

  public JwtClientExample() {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.client.sasl.provider.class",
      "org.apache.hadoop.hbase.security.provider.OAuthBearerSaslProviderSelector");
    conf.set("hbase.client.sasl.provider.extras",
      "org.apache.hadoop.hbase.security.provider.OAuthBearerSaslClientAuthenticationProvider");
    setConf(conf);
  }

  @Override public int run(String[] args) throws Exception {
    LOG.info("JWT client example has been started");

    Configuration conf = getConf();
    LOG.info("Config = " + conf.get("hbase.client.sasl.provider.class"));
    UserProvider provider = UserProvider.instantiate(conf);
    User user = provider.getCurrent();

    OAuthBearerTokenUtil.addTokenForUser(user, JWT_TOKEN, 0);
    LOG.info("JWT token added");

    try (final Connection conn = ConnectionFactory.createConnection(conf, user)) {
      LOG.info("Connected to HBase");
      Admin admin = conn.getAdmin();

      TableName tn = TableName.valueOf("jwt-test-table");
      if (!admin.isTableAvailable(tn)) {
        TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tn)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(FAMILY).build())
          .build();
        admin.createTable(tableDescriptor);
      }

      Table table = conn.getTable(tn);
      byte[] rk = Bytes.toBytes(ThreadLocalRandom.current().nextLong());
      Put p = new Put(rk);
      p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
          .setRow(rk)
          .setFamily(FAMILY)
          .setType(Cell.Type.Put)
          .setValue("test".getBytes(StandardCharsets.UTF_8))
        .build());
      table.put(p);

      admin.disableTable(tn);
      admin.deleteTable(tn);
    }

    LOG.info("JWT client example is done");
    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new JwtClientExample(), args);
  }
}
