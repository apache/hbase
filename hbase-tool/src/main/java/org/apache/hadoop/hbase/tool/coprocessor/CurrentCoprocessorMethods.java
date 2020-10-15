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

package org.apache.hadoop.hbase.tool.coprocessor;

import java.lang.reflect.Method;

import org.apache.hadoop.hbase.coprocessor.BulkLoadObserver;
import org.apache.hadoop.hbase.coprocessor.EndpointObserver;
import org.apache.hadoop.hbase.coprocessor.MasterObserver;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;
import org.apache.hadoop.hbase.coprocessor.WALObserver;
import org.apache.yetus.audience.InterfaceAudience;

@InterfaceAudience.Private
public class CurrentCoprocessorMethods extends CoprocessorMethods {
  public CurrentCoprocessorMethods() {
    addMethods(BulkLoadObserver.class);
    addMethods(EndpointObserver.class);
    addMethods(MasterObserver.class);
    addMethods(RegionObserver.class);
    addMethods(RegionServerObserver.class);
    addMethods(WALObserver.class);
  }

  private void addMethods(Class<?> clazz) {
    for (Method method : clazz.getDeclaredMethods()) {
      addMethod(method);
    }
  }
}
