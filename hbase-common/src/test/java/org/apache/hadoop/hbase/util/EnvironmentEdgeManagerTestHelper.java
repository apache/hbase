/*
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

/**
 * Used by tests to inject an edge into the manager. The intent is to minimise the use of the
 * injectEdge method giving it default permissions, but in testing we may need to use this
 * functionality elsewhere.
 */
public final class EnvironmentEdgeManagerTestHelper {
  private EnvironmentEdgeManagerTestHelper() {
  }

  public static void reset() {
    EnvironmentEdgeManager.reset();
  }

  public static void injectEdge(EnvironmentEdge edge) {
    EnvironmentEdgeManager.injectEdge(edge);
  }

  private static final class PackageEnvironmentEdgeWrapper implements EnvironmentEdge {

    private final EnvironmentEdge delegate;

    private final String packageName;

    PackageEnvironmentEdgeWrapper(EnvironmentEdge delegate, String packageName) {
      this.delegate = delegate;
      this.packageName = packageName;
    }

    @Override
    public long currentTime() {
      StackTraceElement[] elements = new Exception().getStackTrace();
      // the first element is us, the second one is EnvironmentEdgeManager, so let's check the third
      // one
      if (elements.length > 2 && elements[2].getClassName().startsWith(packageName)) {
        return delegate.currentTime();
      } else {
        return System.currentTimeMillis();
      }
    }
  }

  /**
   * Inject a {@link EnvironmentEdge} which only takes effect when calling directly from the classes
   * in the given package.
   */
  public static void injectEdgeForPackage(EnvironmentEdge edge, String packageName) {
    injectEdge(new PackageEnvironmentEdgeWrapper(edge, packageName));
  }
}
