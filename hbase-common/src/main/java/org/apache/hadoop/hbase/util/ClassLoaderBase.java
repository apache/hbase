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

import java.net.URL;
import java.net.URLClassLoader;

import org.apache.yetus.audience.InterfaceAudience;

import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;

/**
 * Base class loader that defines couple shared constants used by sub-classes.
 */
@InterfaceAudience.Private
public class ClassLoaderBase extends URLClassLoader {

  protected static final String DEFAULT_LOCAL_DIR = "/tmp/hbase-local-dir";
  protected static final String LOCAL_DIR_KEY = "hbase.local.dir";

  /**
   * Parent class loader.
   */
  protected final ClassLoader parent;

  /**
   * Creates a DynamicClassLoader that can load classes dynamically
   * from jar files under a specific folder.
   *
   * @param parent the parent ClassLoader to set.
   */
  public ClassLoaderBase(final ClassLoader parent) {
    super(new URL[]{}, parent);
    Preconditions.checkNotNull(parent, "No parent classloader!");
    this.parent = parent;
  }

}
