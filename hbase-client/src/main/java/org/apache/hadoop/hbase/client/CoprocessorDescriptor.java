/**
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
package org.apache.hadoop.hbase.client;

import java.util.Map;
import java.util.Optional;
import org.apache.yetus.audience.InterfaceAudience;

/**
 * CoprocessorDescriptor contains the details about how to build a coprocessor.
 * This class is a pojo so there are no checks for the details carried by this class.
 * Use {@link CoprocessorDescriptorBuilder} to instantiate a CoprocessorDescriptor
 */
@InterfaceAudience.Public
public interface CoprocessorDescriptor {
  /**
   * @return the name of the class or interface represented by this object.
   */
  String getClassName();

  /**
   * @return Path of the jar file. If it's null, the class will be loaded from default classloader.
   */
  Optional<String> getJarPath();

  /**
   * @return The order to execute this coprocessor
   */
  int getPriority();

  /**
   * @return Arbitrary key-value parameter pairs passed into the  coprocessor.
   */
  Map<String, String> getProperties();
}
