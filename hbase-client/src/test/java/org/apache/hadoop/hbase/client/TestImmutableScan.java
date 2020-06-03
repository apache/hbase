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

package org.apache.hadoop.hbase.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.security.access.Permission;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.testclassification.ClientTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Small tests for ImmutableScan
 */
@Category({ ClientTests.class, SmallTests.class })
public class TestImmutableScan {

  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestImmutableScan.class);

  private static final Logger LOG = LoggerFactory.getLogger(TestImmutableScan.class);

  @Test
  public void testScanCopyConstructor() throws Exception {
    Scan scan = new Scan();

    scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("q"))
      .setACL("test_user2", new Permission(Permission.Action.READ))
      .setAllowPartialResults(true)
      .setAsyncPrefetch(false)
      .setAttribute("test_key", Bytes.toBytes("test_value"))
      .setAuthorizations(new Authorizations("test_label"))
      .setBatch(10)
      .setCacheBlocks(false)
      .setCaching(10)
      .setConsistency(Consistency.TIMELINE)
      .setFilter(new FilterList())
      .setId("scan_copy_constructor")
      .setIsolationLevel(IsolationLevel.READ_COMMITTED)
      .setLimit(100)
      .setLoadColumnFamiliesOnDemand(false)
      .setMaxResultSize(100)
      .setMaxResultsPerColumnFamily(1000)
      .readVersions(9999)
      .setMvccReadPoint(5)
      .setNeedCursorResult(true)
      .setPriority(1)
      .setRaw(true)
      .setReplicaId(3)
      .setReversed(true)
      .setRowOffsetPerColumnFamily(5)
      .setRowPrefixFilter(Bytes.toBytes("row_"))
      .setScanMetricsEnabled(true)
      .setSmall(true)
      .setReadType(Scan.ReadType.STREAM)
      .withStartRow(Bytes.toBytes("row_1"))
      .withStopRow(Bytes.toBytes("row_2"))
      .setTimeRange(0, 13);

    // create a copy of existing scan object
    Scan scanCopy = new ImmutableScan(scan);

    // validate fields of copied scan object match with the original scan object
    assertArrayEquals(scan.getACL(), scanCopy.getACL());
    assertEquals(scan.getAllowPartialResults(), scanCopy.getAllowPartialResults());
    assertArrayEquals(scan.getAttribute("test_key"), scanCopy.getAttribute("test_key"));
    assertEquals(scan.getAttributeSize(), scanCopy.getAttributeSize());
    assertEquals(scan.getAttributesMap(), scanCopy.getAttributesMap());
    assertEquals(scan.getAuthorizations().getLabels(), scanCopy.getAuthorizations().getLabels());
    assertEquals(scan.getBatch(), scanCopy.getBatch());
    assertEquals(scan.getCacheBlocks(), scanCopy.getCacheBlocks());
    assertEquals(scan.getCaching(), scanCopy.getCaching());
    assertEquals(scan.getConsistency(), scanCopy.getConsistency());
    assertEquals(scan.getFamilies().length, scanCopy.getFamilies().length);
    assertArrayEquals(scan.getFamilies()[0], scanCopy.getFamilies()[0]);
    assertEquals(scan.getFamilyMap(), scanCopy.getFamilyMap());
    assertEquals(scan.getFilter(), scanCopy.getFilter());
    assertEquals(scan.getId(), scanCopy.getId());
    assertEquals(scan.getIsolationLevel(), scanCopy.getIsolationLevel());
    assertEquals(scan.getLimit(), scanCopy.getLimit());
    assertEquals(scan.getLoadColumnFamiliesOnDemandValue(),
      scanCopy.getLoadColumnFamiliesOnDemandValue());
    assertEquals(scan.getMaxResultSize(), scanCopy.getMaxResultSize());
    assertEquals(scan.getMaxResultsPerColumnFamily(), scanCopy.getMaxResultsPerColumnFamily());
    assertEquals(scan.getMaxVersions(), scanCopy.getMaxVersions());
    assertEquals(scan.getMvccReadPoint(), scanCopy.getMvccReadPoint());
    assertEquals(scan.getPriority(), scanCopy.getPriority());
    assertEquals(scan.getReadType(), scanCopy.getReadType());
    assertEquals(scan.getReplicaId(), scanCopy.getReplicaId());
    assertEquals(scan.getRowOffsetPerColumnFamily(), scanCopy.getRowOffsetPerColumnFamily());
    assertArrayEquals(scan.getStartRow(), scanCopy.getStartRow());
    assertArrayEquals(scan.getStopRow(), scanCopy.getStopRow());
    assertEquals(scan.getTimeRange(), scanCopy.getTimeRange());
    assertEquals(scan.getFingerprint(), scanCopy.getFingerprint());
    assertEquals(scan.toMap(1), scanCopy.toMap(1));
    assertEquals(scan.toString(2), scanCopy.toString(2));
    assertEquals(scan.toJSON(2), scanCopy.toJSON(2));

    LOG.debug("Compare all getters of scan and scanCopy.");
    compareGetters(scan, scanCopy);

    testUnmodifiableSetters(scanCopy);
  }

  private void testUnmodifiableSetters(Scan scanCopy) throws IOException {
    try {
      scanCopy.setFilter(Mockito.mock(Filter.class));
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setFilter", e.getMessage());
    }
    try {
      scanCopy.addFamily(new byte[] { 0, 1 });
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to addFamily", e.getMessage());
    }
    try {
      scanCopy.addColumn(new byte[] { 0, 1 }, new byte[] { 2, 3 });
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to addColumn", e.getMessage());
    }
    try {
      scanCopy.setTimeRange(1L, 2L);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setTimeRange", e.getMessage());
    }
    try {
      scanCopy.setTimestamp(1L);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setTimestamp", e.getMessage());
    }
    try {
      scanCopy.setColumnFamilyTimeRange(new byte[] { 0 }, 1L, 2L);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setColumnFamilyTimeRange",
        e.getMessage());
    }
    try {
      scanCopy.withStopRow(new byte[] { 1, 2 });
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to withStopRow", e.getMessage());
    }
    try {
      scanCopy.setRowPrefixFilter(new byte[] { 1, 2 });
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setRowPrefixFilter", e.getMessage());
    }
    try {
      scanCopy.readAllVersions();
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to readAllVersions", e.getMessage());
    }
    try {
      scanCopy.setBatch(1);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setBatch", e.getMessage());
    }
    try {
      scanCopy.setRowOffsetPerColumnFamily(1);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setRowOffsetPerColumnFamily",
        e.getMessage());
    }
    try {
      scanCopy.setCaching(1);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setCaching",
        e.getMessage());
    }
    try {
      scanCopy.setLoadColumnFamiliesOnDemand(true);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setLoadColumnFamiliesOnDemand",
        e.getMessage());
    }
    try {
      scanCopy.setRaw(true);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setRaw", e.getMessage());
    }
    try {
      scanCopy.setAuthorizations(new Authorizations("test"));
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setAuthorizations", e.getMessage());
    }
    try {
      scanCopy.setACL("user1", new Permission(Permission.Action.READ));
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setACL", e.getMessage());
    }
    try {
      scanCopy.setReplicaId(12);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setReplicaId", e.getMessage());
    }
    try {
      scanCopy.setReadType(Scan.ReadType.STREAM);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setReadType", e.getMessage());
    }
    try {
      scanCopy.setOneRowLimit();
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setOneRowLimit", e.getMessage());
    }
    try {
      scanCopy.setNeedCursorResult(false);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setNeedCursorResult", e.getMessage());
    }
    try {
      scanCopy.resetMvccReadPoint();
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to resetMvccReadPoint", e.getMessage());
    }
    try {
      scanCopy.setMvccReadPoint(1L);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setMvccReadPoint", e.getMessage());
    }
    try {
      scanCopy.setIsolationLevel(IsolationLevel.READ_UNCOMMITTED);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setIsolationLevel", e.getMessage());
    }
    try {
      scanCopy.setPriority(10);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setPriority", e.getMessage());
    }
    try {
      scanCopy.setConsistency(Consistency.TIMELINE);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setConsistency", e.getMessage());
    }
    try {
      scanCopy.setCacheBlocks(true);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setCacheBlocks", e.getMessage());
    }
    try {
      scanCopy.setAllowPartialResults(true);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setAllowPartialResults",
        e.getMessage());
    }
    try {
      scanCopy.setId("id");
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setId", e.getMessage());
    }
    try {
      scanCopy.setMaxResultSize(100);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setMaxResultSize", e.getMessage());
    }
    try {
      scanCopy.setMaxResultsPerColumnFamily(100);
      throw new RuntimeException("Should not reach here");
    } catch (UnsupportedOperationException e) {
      assertEquals("ImmutableScan does not allow access to setMaxResultsPerColumnFamily",
        e.getMessage());
    }
  }

  private void compareGetters(Scan scan, Scan scanCopy) {
    Method[] methods = Scan.class.getMethods();
    for (Method method : methods) {
      if (isGetter(method)) {
        LOG.debug("Comparing return values of method: {}", method);
        try {
          Object obj1;
          Object obj2;
          switch (method.getName()) {
            case "toMap": {
              if (method.getParameterCount() == 1) {
                obj1 = method.invoke(scan, 2);
                obj2 = method.invoke(scanCopy, 2);
                break;
              }
            }
            case "getAttribute": {
              if (method.getParameterCount() == 1) {
                obj1 = method.invoke(scan, "acl");
                obj2 = method.invoke(scanCopy, "acl");
                break;
              }
            }
            case "toString": {
              if (method.getParameterCount() == 1) {
                obj1 = method.invoke(scan, 25);
                obj2 = method.invoke(scanCopy, 25);
                break;
              }
            }
            case "toJSON": {
              if (method.getParameterCount() == 1) {
                obj1 = method.invoke(scan, 25);
                obj2 = method.invoke(scanCopy, 25);
                break;
              }
            }
            default: {
              obj1 = method.invoke(scan);
              obj2 = method.invoke(scanCopy);
            }
          }
          if (obj1 instanceof Map && obj2 instanceof Map) {
            obj1 = Collections.unmodifiableMap((Map<?, ?>) obj1);
          }
          if (!EqualsBuilder.reflectionEquals(obj1, obj2)) {
            throw new AssertionError("Method " + method + " does not return equal values");
          }
        } catch (IllegalAccessException | InvocationTargetException | IllegalArgumentException e) {
          throw new AssertionError("Error invoking method " + method, e);
        }
      }
    }
  }

  private static boolean isGetter(Method method) {
    if ("hashCode".equals(method.getName()) || "equals".equals(method.getName())
        || method.getName().startsWith("set")) {
      return false;
    }
    return !void.class.equals(method.getReturnType())
      && !Scan.class.equals(method.getReturnType());
  }

}
