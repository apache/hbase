/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package org.apache.hadoop.hbase.security.visibility;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;


/**
 * Utility methods for testing visibility labels.
 */
public class VisibilityTestUtil {

  public static void enableVisiblityLabels(Configuration conf) throws IOException {
    conf.setInt("hfile.format.version", 3);
    appendCoprocessor(conf, CoprocessorHost.MASTER_COPROCESSOR_CONF_KEY,
      VisibilityController.class.getName());
    appendCoprocessor(conf, CoprocessorHost.REGION_COPROCESSOR_CONF_KEY,
      VisibilityController.class.getName());
  }

  private static void appendCoprocessor(Configuration conf, String property, String value) {
    if (conf.get(property) == null) {
      conf.set(property, VisibilityController.class.getName());
    } else {
      conf.set(property, conf.get(property) + "," + VisibilityController.class.getName());
    }
  }

}
