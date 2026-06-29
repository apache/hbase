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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hadoop.hbase.testclassification.MiscTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(MiscTests.TAG)
@Tag(SmallTests.TAG)
public class TestStrings {

  @Test
  public void testHostnamesEqual() {
    assertTrue(Strings.hostnamesEqual("HOST.example.com", "host.example.com"));
    assertTrue(Strings.hostnamesEqual("rs1", "RS1"));
    assertFalse(Strings.hostnamesEqual("host-a.example.com", "host-b.example.com"));
    assertTrue(Strings.hostnamesEqual("10.0.0.1", "10.0.0.1"));
    assertFalse(Strings.hostnamesEqual("10.0.0.1", "10.0.0.2"));
    assertFalse(Strings.hostnamesEqual("HOST.example.com", "10.0.0.1"));
    assertTrue(Strings.hostnamesEqual("::1", "0:0:0:0:0:0:0:1"));
    assertTrue(Strings.hostnamesEqual("[2001:0db8:85a3:0000:0000:8a2e:0370:7334]",
      "2001:0db8:85a3:0000:0000:8a2e:0370:7334"));
    assertThrows(NullPointerException.class, () -> Strings.hostnamesEqual(null, "host"));
    assertThrows(NullPointerException.class, () -> Strings.hostnamesEqual("host", null));
  }

}
