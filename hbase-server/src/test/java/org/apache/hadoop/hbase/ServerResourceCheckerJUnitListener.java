/*
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

package org.apache.hadoop.hbase;

import org.apache.hadoop.hbase.ResourceChecker.Phase;
import org.apache.hadoop.hbase.client.HConnectionTestingUtility;

/**
 * Monitor the resources. use by the tests All resources in {@link ResourceCheckerJUnitListener}
 *  plus the number of connection.
 */
public class ServerResourceCheckerJUnitListener extends ResourceCheckerJUnitListener {

  static class ConnectionCountResourceAnalyzer extends ResourceChecker.ResourceAnalyzer {
    @Override
    public int getVal(Phase phase) {
      return HConnectionTestingUtility.getConnectionCount();
    }
  }

  @Override
  protected void addResourceAnalyzer(ResourceChecker rc) {
    rc.addResourceAnalyzer(new ConnectionCountResourceAnalyzer());
  }
}
