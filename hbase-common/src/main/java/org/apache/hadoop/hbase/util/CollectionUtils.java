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

import org.apache.yetus.audience.InterfaceAudience;

/**
 * Utility methods for dealing with Collections, including treating null collections as empty.
 * @deprecated Since 2.0.6/2.1.3/2.2.0
 */
@InterfaceAudience.Private
//HBASE-20215: Marking as deprecated. Although audience is private, there might be references to
//this class on some client classpath.
@Deprecated
public class CollectionUtils extends ConcurrentMapUtils {

}
