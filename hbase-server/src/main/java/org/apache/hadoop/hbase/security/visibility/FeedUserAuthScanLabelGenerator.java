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

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

/**
 * If the passed in authorization is null, then this ScanLabelGenerator
 * feeds the set of predefined authorization labels for the given user. That is
 * the set defined by the admin using the VisibilityClient admin interface
 * or the set_auths shell command.
 * Otherwise the passed in authorization labels are returned with no change.
 *
 * Note: This SLG should not be used alone because it does not check
 * the passed in authorization labels against what the user is authorized for.
 */
@InterfaceAudience.Private
public class FeedUserAuthScanLabelGenerator implements ScanLabelGenerator {

  private static final Log LOG = LogFactory.getLog(FeedUserAuthScanLabelGenerator.class);

  private Configuration conf;
  private VisibilityLabelsCache labelsCache;

  public FeedUserAuthScanLabelGenerator() {
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
    if (authorizations == null || authorizations.getLabels() == null
        || authorizations.getLabels().isEmpty()) {
      String userName = user.getShortName();
      return this.labelsCache.getAuths(userName);
    }
    return authorizations.getLabels();
  }

}
