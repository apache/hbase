/*
 * Copyright 2010 The Apache Software Foundation
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
package org.apache.hadoop.hbase.regionserver;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.idx.IdxColumnDescriptor;
import org.apache.hadoop.hbase.client.idx.IdxIndexDescriptor;
import org.apache.hadoop.hbase.util.Bytes;

import javax.management.AttributeNotFoundException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.StandardMBean;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * A delegate MBean to get stats / poke an idx region.
 */
public class IdxRegionMBeanImpl extends StandardMBean
  implements IdxRegionMBean {
  static final Log LOG = LogFactory.getLog(IdxRegionMBeanImpl.class);

  private static final HashMap<String, String> ATTRIBUTE_DESCRIPTIONS =
    new HashMap<String, String>() {{
      put("Valid", "Indicates whether the region being exposed by " +
        "this MBean is still alive");
      put("NumberOfIndexedKeys", "The number of keys in the index " +
        "which is equivalent to the number of top-level rows in this region");
      put("IndexesTotalHeapSize", "The total heap size, in bytes, used by " +
        "the indexes and their overhead");
    }};


  /**
   * Instantiate an new IdxRegion MBean. this is a convenience method which
   * allows the caller to avoid catching the
   * {@link javax.management.NotCompliantMBeanException}.
   *
   * @param idxRegion the region to wrap
   * @return a new instance of IdxRegionMBeanImpl
   */
  static IdxRegionMBeanImpl newIdxRegionMBeanImpl(IdxRegion idxRegion) {
    try {
      return new IdxRegionMBeanImpl(idxRegion);
    } catch (NotCompliantMBeanException e) {
      throw new IllegalStateException("Could not instantiate mbean", e);
    }
  }

  /**
   * Generate object name from the hregion info.
   *
   * @param regionInfo the region info to create the object name from.
   * @return an valid object name.
   */
  static ObjectName generateObjectName(HRegionInfo regionInfo) {
    StringBuilder builder =
      new StringBuilder(IdxRegionMBeanImpl.class.getPackage().getName());
    builder.append(':');
    builder.append("table=");
    builder.append(regionInfo.getTableDesc().getNameAsString());
    builder.append(',');

    builder.append("id=");
    builder.append(regionInfo.getRegionId());
    builder.append(',');

    if (regionInfo.getStartKey() != null &&
      regionInfo.getStartKey().length > 0) {
      builder.append("startKey=");
      builder.append(Bytes.toString(regionInfo.getStartKey()));
      builder.append(',');
    }

    if (regionInfo.getEndKey() != null &&
      regionInfo.getEndKey().length > 0) {
      builder.append("endKey=");
      builder.append(Bytes.toString(regionInfo.getEndKey()));
      builder.append(',');
    }

    builder.append("type=IdxRegion");
    try {
      return ObjectName.getInstance(builder.toString());
    } catch (MalformedObjectNameException e) {
      throw new IllegalStateException("Failed to create a legal object name",
        e);
    }
  }

  /**
   * Using a weak reference to the idx region.
   * This way a failure to clear this MBean from the mbean server won't prevent
   * the idx region to be garbage collected.
   */
  private WeakReference<IdxRegion> idxRegionRef;

  private IdxRegionMBeanImpl(IdxRegion idxRegion)
    throws NotCompliantMBeanException {
    super(IdxRegionMBean.class);
    idxRegionRef = new WeakReference<IdxRegion>(idxRegion);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getNumberOfIndexedKeys() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.getNumberOfIndexedKeys();
    } else {
      return -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getIndexesTotalHeapSize() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.getIndexesTotalHeapSize();
    } else {
      return -1;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isValid() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return !(region.isClosed() || region.isClosing());
    } else {
      return false;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getTotalIndexedScans() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.getTotalIndexedScans();
    } else {
      return -1L;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long resetTotalIndexedScans() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.resetTotalIndexedScans();
    } else {
      return -1L;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getTotalNonIndexedScans() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.getTotalNonIndexedScans();
    } else {
      return -1L;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long resetTotalNonIndexedScans() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.resetTotalNonIndexedScans();
    } else {
      return -1L;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getNumberOfOngoingIndexedScans() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.getNumberOfOngoingIndexedScans();
    } else {
      return -1L;
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getIndexBuildTimes() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return toIndexBuildTimesString(region.getIndexBuildTimes());
    } else {
      return "";
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String resetIndexBuildTimes() {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return toIndexBuildTimesString(region.resetIndexBuildTimes());
    } else {
      return "";
    }
  }

  private String toIndexBuildTimesString(long[] buildTimes) {
    StringBuilder builder = new StringBuilder();
    for (long indexBuildTime : buildTimes) {
      if (indexBuildTime >= 0) {
        builder.append(indexBuildTime);
        builder.append(", ");
      }
    }
    return builder.length() > 2 ? builder.substring(0, builder.length() - 2) :
      "";
  }

/* StandardMBean hooks and overrides */

  @Override
  protected String getDescription(MBeanAttributeInfo info) {
    String description = ATTRIBUTE_DESCRIPTIONS.get(info.getName());
    return description == null ? super.getDescription(info) : description;
  }

  @Override
  public Object getAttribute(String attribute)
    throws AttributeNotFoundException, MBeanException, ReflectionException {
    if (attribute.endsWith(".heapSize")) {
      String columnName = attribute.substring(0, attribute.indexOf('.'));
      return getIndexHeapSize(columnName);
    }
    return super.getAttribute(attribute);
  }

  @Override
  protected void cacheMBeanInfo(MBeanInfo info) {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      Set<String> columnNames;
      try {
        columnNames = extractIndexedColumnNames(region.getRegionInfo());
      } catch (IOException e) {
        throw new IllegalStateException("Invalid region info for " + region);
      }
      final MBeanAttributeInfo[] existingInfos = info.getAttributes();
      for (MBeanAttributeInfo attributeInfo : existingInfos) {
        String name = attributeInfo.getName();
        if (name.indexOf('.') >= 0) {
          columnNames.remove(name.substring(0, name.indexOf('.')));
        }
      }
      MBeanAttributeInfo[] attributeInfos = new
        MBeanAttributeInfo[columnNames.size() + existingInfos.length];
      System.arraycopy(existingInfos, 0, attributeInfos, 0,
        existingInfos.length);
      Iterator<String> columnNameIterator = columnNames.iterator();
      for (int i = existingInfos.length; i < attributeInfos.length; i++) {
        String name = columnNameIterator.next() + ".heapSize";
        attributeInfos[i] = new MBeanAttributeInfo(name,
          "long", "The amount of heap space occupied by this index", true,
          false, false);
      }
      info = new MBeanInfo(info.getClassName(), info.getDescription(),
        attributeInfos, info.getConstructors(), info.getOperations(),
        info.getNotifications(), info.getDescriptor());
    }
    super.cacheMBeanInfo(info);
  }

  private static Set<String> extractIndexedColumnNames(HRegionInfo regionInfo)
    throws IOException {
    Set<String> idxColumns = new HashSet<String>();
    for (HColumnDescriptor columnDescriptor :
      regionInfo.getTableDesc().getColumnFamilies()) {
      Collection<IdxIndexDescriptor> indexDescriptors =
        IdxColumnDescriptor.getIndexDescriptors(columnDescriptor).values();
      for (IdxIndexDescriptor indexDescriptor : indexDescriptors) {
        idxColumns.add(columnDescriptor.getNameAsString() + ":" +
          Bytes.toString(indexDescriptor.getQualifierName()));
      }
    }
    return idxColumns;
  }

  private long getIndexHeapSize(String columnName) {
    IdxRegion region = idxRegionRef.get();
    if (region != null) {
      return region.getIndexHeapSize(columnName);
    } else {
      return -1L;
    }
  }
}
