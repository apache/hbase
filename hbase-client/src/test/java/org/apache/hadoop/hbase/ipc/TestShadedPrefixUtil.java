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
package org.apache.hadoop.hbase.ipc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(ClientTests.TAG)
@Tag(SmallTests.TAG)
public class TestShadedPrefixUtil {

  private static final String EXCEPTION_CLASS =
    "org.apache.hadoop.hbase.exceptions.RegionMovedException";

  @Test
  public void testApplyShadingNotShaded() {
    ShadedPrefixUtil util = ShadedPrefixUtil.getInstance();
    assertNull(util.getShadedBase());
    assertEquals(EXCEPTION_CLASS, util.applyShading(EXCEPTION_CLASS));
    assertNull(util.applyShading(null));
  }

  @Test
  public void testApplyShadingPrependPrefix() {
    ShadedPrefixUtil util =
      ShadedPrefixUtil.newInstance("a.b.c." + ShadedPrefixUtil.class.getPackage().getName());
    assertEquals("a.b.c.org.apache.hadoop.hbase", util.getShadedBase());
    assertEquals("a.b.c." + EXCEPTION_CLASS, util.applyShading(EXCEPTION_CLASS));
    assertNull(util.applyShading(null));
  }

  @Test
  public void testApplyShadingFullReplacement() {
    ShadedPrefixUtil util = ShadedPrefixUtil.newInstance("shaded.hbase1.ipc");
    assertEquals("shaded.hbase1", util.getShadedBase());
    assertEquals("shaded.hbase1.exceptions.RegionMovedException",
      util.applyShading(EXCEPTION_CLASS));
    assertNull(util.applyShading(null));
  }

  @Test
  public void testApplyShadingNonHBaseClass() {
    ShadedPrefixUtil util = ShadedPrefixUtil.newInstance("shaded.hbase1.ipc");
    assertEquals("com.example.SomeClass", util.applyShading("com.example.SomeClass"));
  }

  @Test
  public void testApplyShadingNullPackage() {
    ShadedPrefixUtil util = ShadedPrefixUtil.newInstance(null);
    assertNull(util.getShadedBase());
    assertEquals(EXCEPTION_CLASS, util.applyShading(EXCEPTION_CLASS));
  }

  @Test
  public void testApplyShadingUnrecognizablePackage() {
    ShadedPrefixUtil util = ShadedPrefixUtil.newInstance("some.unrelated.package");
    assertNull(util.getShadedBase());
    assertEquals(EXCEPTION_CLASS, util.applyShading(EXCEPTION_CLASS));
  }

  @Test
  public void testResolveShadingNotShaded() {
    ShadedPrefixUtil util = ShadedPrefixUtil.getInstance();
    // No shading: returned as-is without class loading
    assertEquals(EXCEPTION_CLASS, util.resolveShading(EXCEPTION_CLASS));
    assertNull(util.resolveShading(null));
  }

  @Test
  public void testResolveShadingFallsBackWhenShadedClassMissing() {
    // In the test classpath no "shaded.hbase1.*" classes exist, so resolveShading must
    // fall back to the original name rather than the transformed one.
    ShadedPrefixUtil util = ShadedPrefixUtil.newInstance("shaded.hbase1.ipc");
    assertEquals(EXCEPTION_CLASS, util.resolveShading(EXCEPTION_CLASS));
  }

  @Test
  public void testResolveShadingCachesResult() {
    ShadedPrefixUtil util = ShadedPrefixUtil.newInstance("shaded.hbase1.ipc");
    String first = util.resolveShading(EXCEPTION_CLASS);
    String second = util.resolveShading(EXCEPTION_CLASS);
    // Identical object reference confirms the cache returned the memoized result
    assertSame(first, second);
  }
}
