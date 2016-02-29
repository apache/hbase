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
package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.util.ReflectionUtils;

/**
 * A singleton store segment factory.
 * Generate concrete store segments.
 */
@InterfaceAudience.Private
public final class SegmentFactory {

  static final String USEMSLAB_KEY = "hbase.hregion.memstore.mslab.enabled";
  static final boolean USEMSLAB_DEFAULT = true;
  static final String MSLAB_CLASS_NAME = "hbase.regionserver.mslab.class";

  private SegmentFactory() {}
  private static SegmentFactory instance = new SegmentFactory();
  public static SegmentFactory instance() {
    return instance;
  }

  public ImmutableSegment createImmutableSegment(final Configuration conf,
      final CellComparator comparator, long size) {
    MemStoreLAB memStoreLAB = getMemStoreLAB(conf);
    MutableSegment segment = generateMutableSegment(conf, comparator, memStoreLAB, size);
    return createImmutableSegment(conf, segment);
  }

  public ImmutableSegment createImmutableSegment(CellComparator comparator,
      long size) {
    MutableSegment segment = generateMutableSegment(null, comparator, null, size);
    return createImmutableSegment(null, segment);
  }

  public ImmutableSegment createImmutableSegment(final Configuration conf, MutableSegment segment) {
    return new ImmutableSegment(segment);
  }
  public MutableSegment createMutableSegment(final Configuration conf,
      CellComparator comparator, long size) {
    MemStoreLAB memStoreLAB = getMemStoreLAB(conf);
    return generateMutableSegment(conf, comparator, memStoreLAB, size);
  }

  //****** private methods to instantiate concrete store segments **********//

  private MutableSegment generateMutableSegment(
      final Configuration conf, CellComparator comparator, MemStoreLAB memStoreLAB, long size) {
    // TBD use configuration to set type of segment
    CellSet set = new CellSet(comparator);
    return new MutableSegment(set, comparator, memStoreLAB, size);
  }

  private MemStoreLAB getMemStoreLAB(Configuration conf) {
    MemStoreLAB memStoreLAB = null;
    if (conf.getBoolean(USEMSLAB_KEY, USEMSLAB_DEFAULT)) {
      String className = conf.get(MSLAB_CLASS_NAME, HeapMemStoreLAB.class.getName());
      memStoreLAB = ReflectionUtils.instantiateWithCustomCtor(className,
          new Class[] { Configuration.class }, new Object[] { conf });
    }
    return memStoreLAB;
  }

}
