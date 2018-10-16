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
package org.apache.hadoop.hbase.security.visibility;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

/**
 * This ScanLabelGenerator enforces a set of predefined authorizations for a
 * given user, the set defined by the admin using the VisibilityClient admin
 * interface or the set_auths shell command. Any authorizations requested with
 * Scan#authorizations will be ignored.
 */
@InterfaceAudience.Private
public class EnforcingScanLabelGenerator implements ScanLabelGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(EnforcingScanLabelGenerator.class);

  private Configuration conf;
  private VisibilityLabelsCache labelsCache;

  public EnforcingScanLabelGenerator() {
    this.labelsCache = VisibilityLabelsCache.get();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public List<String> getLabels(User user, Authorizations authorizations) {
    String userName = user.getShortName();
    if (authorizations != null) {
      LOG.warn("Dropping authorizations requested by user " + userName + ": " + authorizations);
    }
    Set<String> auths = new HashSet<>();
    auths.addAll(this.labelsCache.getUserAuths(userName));
    auths.addAll(this.labelsCache.getGroupAuths(user.getGroupNames()));
    return new ArrayList<>(auths);
  }

}
