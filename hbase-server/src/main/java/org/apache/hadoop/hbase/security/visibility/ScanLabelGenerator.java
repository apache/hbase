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

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.hbase.security.User;

/**
 * This would be the interface which would be used add labels to the RPC context
 * and this would be stored against the UGI.
 *
 */
@InterfaceAudience.Public
public interface ScanLabelGenerator extends Configurable {

  /**
   * Helps to get a list of lables associated with an UGI
   * @param user
   * @param authorizations
   * @return The labels 
   */
  public List<String> getLabels(User user, Authorizations authorizations);
}
