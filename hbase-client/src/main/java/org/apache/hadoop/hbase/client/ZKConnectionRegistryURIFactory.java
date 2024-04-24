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
package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.hbase.zookeeper.ReadOnlyZKClient;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection registry creator implementation for creating {@link ZKConnectionRegistry}.
 */
@InterfaceAudience.Private
public class ZKConnectionRegistryURIFactory implements ConnectionRegistryURIFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ZKConnectionRegistryURIFactory.class);

  public static final String SESSION_TIMEOUT = "session.timeout.ms";

  public static final String RECOVERY_RETRY = "recovery.retry";

  public static final String RECOVERY_RETRY_INTERVAL_MILLIS = "recovery.retry.interval.ms";

  public static final String KEEPALIVE_MILLIS = "keep-alive.time.ms";

  private void setConf(Map<String, String> queries, Configuration conf, String queryName,
    String confName) {
    String value = queries.get(queryName);
    if (!StringUtils.isBlank(value)) {
      conf.set(confName, value);
    }
  }

  private void parseQueries(URI uri, Configuration conf) {
    Map<String, String> queries = Strings.parseURIQueries(uri);
    setConf(queries, conf, SESSION_TIMEOUT, HConstants.ZK_SESSION_TIMEOUT);
    setConf(queries, conf, RECOVERY_RETRY, ReadOnlyZKClient.RECOVERY_RETRY);
    setConf(queries, conf, RECOVERY_RETRY_INTERVAL_MILLIS,
      ReadOnlyZKClient.RECOVERY_RETRY_INTERVAL_MILLIS);
    setConf(queries, conf, KEEPALIVE_MILLIS, ReadOnlyZKClient.KEEPALIVE_MILLIS);
  }

  @Override
  public ConnectionRegistry create(URI uri, Configuration conf, User user) throws IOException {
    assert getScheme().equals(uri.getScheme());
    LOG.debug("connect to hbase cluster with zk quorum='{}' and parent='{}'", uri.getAuthority(),
      uri.getPath());
    Configuration c = new Configuration(conf);
    c.set(HConstants.CLIENT_ZOOKEEPER_QUORUM, uri.getAuthority());
    c.set(HConstants.ZOOKEEPER_ZNODE_PARENT, uri.getPath());
    parseQueries(uri, c);
    return new ZKConnectionRegistry(c, user);
  }

  @Override
  public String getScheme() {
    return "hbase+zk";
  }
}
