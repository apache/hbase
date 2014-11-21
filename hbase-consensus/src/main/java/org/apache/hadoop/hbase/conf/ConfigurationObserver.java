/**
 * Copyright 2013 The Apache Software Foundation
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
package org.apache.hadoop.hbase.conf;

import org.apache.hadoop.conf.Configuration;

/**
 * Every class that wants to observe online changes in Configuration properties,
 * must implement interface (and also, register itself with the
 * <code>ConfigurationManager</code> object.
 */
public interface ConfigurationObserver {

  /**
   * This method would be called by the <code>ConfigurationManager</code>
   * object when the <code>Configuration</code> object is reloaded from disk.
   */
  void notifyOnChange(Configuration conf);
}
