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
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Connection registry creator implementation for creating {@link RpcConnectionRegistry}.
 */
@InterfaceAudience.Private
public class RpcConnectionRegistryURIFactory implements ConnectionRegistryURIFactory {

  private static final Logger LOG = LoggerFactory.getLogger(RpcConnectionRegistryURIFactory.class);

  public static final String HEDGED_REQS_FANOUT_KEY = "hedged.fanout";

  public static final String INITIAL_REFRESH_DELAY_SECS = "initial_refresh_delay_secs";

  public static final String PERIODIC_REFRESH_INTERVAL_SECS = "refresh_interval_secs";

  public static final String MIN_SECS_BETWEEN_REFRESHES = "min_secs_between_refreshes";

  private void setConf(Map<String, String> queries, Configuration conf, String queryName,
    String confName) {
    String value = queries.get(queryName);
    if (!StringUtils.isBlank(value)) {
      conf.set(confName, value);
    }
  }

  private void parseQueries(URI uri, Configuration conf) {
    Map<String, String> queries = Strings.parseURIQueries(uri);
    setConf(queries, conf, HEDGED_REQS_FANOUT_KEY, RpcConnectionRegistry.HEDGED_REQS_FANOUT_KEY);
    setConf(queries, conf, INITIAL_REFRESH_DELAY_SECS,
      RpcConnectionRegistry.INITIAL_REFRESH_DELAY_SECS);
    setConf(queries, conf, PERIODIC_REFRESH_INTERVAL_SECS,
      RpcConnectionRegistry.PERIODIC_REFRESH_INTERVAL_SECS);
    setConf(queries, conf, MIN_SECS_BETWEEN_REFRESHES,
      RpcConnectionRegistry.MIN_SECS_BETWEEN_REFRESHES);
  }

  @Override
  public ConnectionRegistry create(URI uri, Configuration conf, User user) throws IOException {
    assert getScheme().equals(uri.getScheme());
    LOG.debug("connect to hbase cluster with rpc bootstrap servers='{}'", uri.getAuthority());
    Configuration c = new Configuration(conf);
    c.set(RpcConnectionRegistry.BOOTSTRAP_NODES, uri.getAuthority());
    parseQueries(uri, c);
    return new RpcConnectionRegistry(c, user);
  }

  @Override
  public String getScheme() {
    return "hbase+rpc";
  }
}
