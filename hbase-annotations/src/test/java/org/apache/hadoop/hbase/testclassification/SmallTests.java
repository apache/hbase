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
package org.apache.hadoop.hbase.testclassification;

/**
 * Tagging a test as 'small' means that the test class has the following characteristics:
 * <ul>
 *  <li>it can be run simultaneously with other small tests all in the same JVM</li>
 *  <li>ideally, the WHOLE implementing test-suite/class, no matter how many or how few test
 *  methods it has, should take less than 15 seconds to complete</li>
 *  <li>it does not use a cluster</li>
 * </ul>
 *
 * @see MediumTests
 * @see LargeTests
 * @see IntegrationTests
 */
public interface SmallTests {}
