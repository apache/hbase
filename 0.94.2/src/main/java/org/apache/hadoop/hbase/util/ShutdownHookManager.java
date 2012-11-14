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
package org.apache.hadoop.hbase.util;

import java.io.IOException;

/**
 * This class provides ShutdownHookManager shims for HBase to interact with the Hadoop 1.0.x and the
 * Hadoop 2.0+ series.
 * 
 * NOTE: No testing done against 0.22.x, or 0.21.x.
 */
abstract public class ShutdownHookManager {
  private static ShutdownHookManager instance;

  static Class shutdownHookManagerClass = null;
  static {
    try {
      // This class exists in hadoop 2.0+ but not in Hadoop 20.x/1.x
      shutdownHookManagerClass = Class.forName("org.apache.hadoop.util.ShutdownHookManager");
      instance = new ShutdownHookManagerV2();
    } catch (Exception e) {
      instance = new ShutdownHookManagerV1();
    }
  }

  abstract public void addShutdownHook(Thread shutdownHook, int priority);
  
  abstract public boolean removeShutdownHook(Runnable shutdownHook);
    
  public static void affixShutdownHook(Thread shutdownHook, int priority) {
    instance.addShutdownHook(shutdownHook, priority);
  }
  
  public static boolean deleteShutdownHook(Runnable shutdownHook) {
    return instance.removeShutdownHook(shutdownHook);
  }

  private static class ShutdownHookManagerV1 extends ShutdownHookManager {
    // priority is ignored in hadoop versions earlier than 2.0
    public void addShutdownHook(Thread shutdownHookThread, int priority) {      
      Runtime.getRuntime().addShutdownHook(shutdownHookThread);
    }
    
    public boolean removeShutdownHook(Runnable shutdownHook) {
      Thread shutdownHookThread = null;
      if (!(shutdownHook instanceof Thread)) {
        shutdownHookThread = new Thread(shutdownHook);
      } else shutdownHookThread = (Thread) shutdownHook;
      
      return Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
    }
  };

  private static class ShutdownHookManagerV2 extends ShutdownHookManager {
    public void addShutdownHook(Thread shutdownHookThread, int priority) {
      try {
        Methods.call(shutdownHookManagerClass, 
            Methods.call(shutdownHookManagerClass, null, "get", null, null),
            "addShutdownHook",
            new Class[] { Runnable.class, int.class },
            new Object[] { shutdownHookThread, priority });
      } catch (Exception ex) {
        throw new RuntimeException("we could not use ShutdownHookManager.addShutdownHook", ex);
      }
    }
    
    public boolean removeShutdownHook(Runnable shutdownHook) {
      try {
        return (Boolean)
        Methods.call(shutdownHookManagerClass, 
            Methods.call(shutdownHookManagerClass, null, "get", null, null),
            "removeShutdownHook",
            new Class[] { Runnable.class },
            new Object[] { shutdownHook });
      } catch (Exception ex) {
        throw new RuntimeException("we could not use ShutdownHookManager", ex);
      }
    }
  };

}
