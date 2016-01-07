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

/**
 * Tag a test as 'large', meaning that the test class has the following
 * characteristics:
 *  - executed in an isolated JVM. Tests can however be executed in different
 *    JVM on the same machine simultaneously.
 *  - will not have to be executed by the developer before submitting a bug
 *  - ideally, last less than 2 minutes to help parallelization
 *
 *  It the worst case compared to small or medium, use it only for tests that
 *    you cannot put in the other categories
 *
 * @see SmallTests
 * @see MediumTests
 * @see IntegrationTests
 */
public interface LargeTests {
}
