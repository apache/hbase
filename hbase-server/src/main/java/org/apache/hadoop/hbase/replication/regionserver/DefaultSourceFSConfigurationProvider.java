/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.replication.regionserver;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.classification.InterfaceAudience;

/**
 * This will load all the xml configuration files for the source cluster replication ID from
 * user configured replication configuration directory.
 */
@InterfaceAudience.Private
public class DefaultSourceFSConfigurationProvider implements SourceFSConfigurationProvider {
  private static final Log LOG = LogFactory.getLog(DefaultSourceFSConfigurationProvider.class);
  // Map containing all the source clusters configurations against their replication cluster id
  private Map<String, Configuration> sourceClustersConfs = new HashMap<>();
  private static final String XML = ".xml";

  @Override
  public Configuration getConf(Configuration sinkConf, String replicationClusterId)
      throws IOException {
    if (sourceClustersConfs.get(replicationClusterId) == null) {
      synchronized (this.sourceClustersConfs) {
        if (sourceClustersConfs.get(replicationClusterId) == null) {
          LOG.info("Loading source cluster FS client conf for cluster " + replicationClusterId);
          // Load only user provided client configurations.
          Configuration sourceClusterConf = new Configuration(false);

          String replicationConfDir = sinkConf.get(HConstants.REPLICATION_CONF_DIR);
          if (replicationConfDir == null) {
            LOG.debug(HConstants.REPLICATION_CONF_DIR + " is not configured.");
            URL resource = HBaseConfiguration.class.getClassLoader().getResource("hbase-site.xml");
            if (resource != null) {
              String path = resource.getPath();
              replicationConfDir = path.substring(0, path.lastIndexOf("/"));
            } else {
              replicationConfDir = System.getenv("HBASE_CONF_DIR");
            }
          }

          LOG.info("Loading source cluster " + replicationClusterId
              + " file system configurations from xml files under directory " + replicationConfDir);
          File confDir = new File(replicationConfDir, replicationClusterId);
          String[] listofConfFiles = FileUtil.list(confDir);
          for (String confFile : listofConfFiles) {
            if (new File(confDir, confFile).isFile() && confFile.endsWith(XML)) {
              // Add all the user provided client conf files
              sourceClusterConf.addResource(new Path(confDir.getPath(), confFile));
            }
          }
          this.sourceClustersConfs.put(replicationClusterId, sourceClusterConf);
        }
      }
    }
    return this.sourceClustersConfs.get(replicationClusterId);
  }

}
