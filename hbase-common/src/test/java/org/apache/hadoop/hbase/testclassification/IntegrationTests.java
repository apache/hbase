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
 * Tag a test as 'integration/system' test, meaning that the test class has the following
 * characteristics: <ul>
 *  <li> Possibly takes hours to complete</li>
 *  <li> Can be run on a mini cluster or an actual cluster</li>
 *  <li> Can make changes to the given cluster (starting stopping daemons, etc)</li>
 *  <li> Should not be run in parallel of other integration tests</li>
 * </ul>
 *
 * Integration / System tests should have a class name starting with "IntegrationTest", and
 * should be annotated with @Category(IntegrationTests.class). Integration tests can be run
 * using the IntegrationTestsDriver class or from mvn verify.
 *
 * @see SmallTests
 * @see MediumTests
 * @see LargeTests
 */
public interface IntegrationTests {
}
