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
package org.apache.hadoop.hbase.master.balancer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.testclassification.MasterTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category({MasterTests.class, SmallTests.class})
public class TestServerAndLoad {

  @Test
  public void test() {
    ServerName server = ServerName.valueOf("host", 12345, 112244);
    int startcode = 12;
    
    ServerAndLoad sal = new ServerAndLoad(server, startcode);
    assertEquals(sal.hashCode(), new ServerAndLoad(server, startcode).hashCode());
    assertEquals(sal, new ServerAndLoad(server, startcode));
    
    assertNotEquals(sal.hashCode(), new ServerAndLoad(server, startcode + 1).hashCode());
    assertNotEquals(sal, new ServerAndLoad(server, startcode + 1));

    ServerName other = ServerName.valueOf("other", 12345, 112244);
    assertNotEquals(sal.hashCode(), new ServerAndLoad(other, startcode).hashCode());
    assertNotEquals(sal, new ServerAndLoad(other, startcode));
   }

}
