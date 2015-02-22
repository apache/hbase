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
import java.util.List;

import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.security.User;

// Strictly removes a specified label
@InterfaceAudience.Private
public class LabelFilteringScanLabelGenerator implements ScanLabelGenerator {

  public static String labelToFilter = null;

  @Override
  public Configuration getConf() {
    return null;
  }

  @Override
  public void setConf(Configuration conf) {

  }

  @Override
  public List<String> getLabels(User user, Authorizations authorizations) {
    if (authorizations != null) {
      if (labelToFilter == null) return authorizations.getLabels();
      List<String> newAuths = new ArrayList<String>();
      for (String auth : authorizations.getLabels()) {
        if (!labelToFilter.equals(auth)) newAuths.add(auth);
      }
      return newAuths;
    }
    return null;
  }
}
