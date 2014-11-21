/**
 * Copyright The Apache Software Foundation
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
package org.apache.hadoop.hbase.util;

import java.util.concurrent.ThreadFactory;

public class DaemonThreadFactory implements ThreadFactory {
  final ThreadGroup group;
  int threadNumber = 1;
  final String namePrefix;

  public DaemonThreadFactory(String namePrefix) {
      SecurityManager s = System.getSecurityManager();
      group = (s != null)? s.getThreadGroup() :
                           Thread.currentThread().getThreadGroup();
      this.namePrefix = namePrefix;
  }

  public Thread newThread(Runnable r) {
      Thread t = new Thread(group, r,
                            namePrefix + (threadNumber++),
                            0);
      if (!t.isDaemon())
          t.setDaemon(true);
      if (t.getPriority() != Thread.NORM_PRIORITY)
          t.setPriority(Thread.NORM_PRIORITY);
      return t;
  }
}
