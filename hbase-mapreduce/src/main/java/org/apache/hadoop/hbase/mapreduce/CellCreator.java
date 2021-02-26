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
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.List;

import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.util.ReflectionUtils;

/**
 * Facade to create Cells for HFileOutputFormat. The created Cells are of <code>Put</code> type.
 */
@InterfaceAudience.Public
public class CellCreator {

  public static final String VISIBILITY_EXP_RESOLVER_CLASS =
      "hbase.mapreduce.visibility.expression.resolver.class";

  private VisibilityExpressionResolver visExpResolver;

  public CellCreator(Configuration conf) {
    Class<? extends VisibilityExpressionResolver> clazz = conf.getClass(
        VISIBILITY_EXP_RESOLVER_CLASS, DefaultVisibilityExpressionResolver.class,
        VisibilityExpressionResolver.class);
    this.visExpResolver = ReflectionUtils.newInstance(clazz, conf);
    this.visExpResolver.init();
  }

  /**
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @return created Cell
   * @throws IOException
   */
  public Cell create(byte[] row, int roffset, int rlength, byte[] family, int foffset, int flength,
      byte[] qualifier, int qoffset, int qlength, long timestamp, byte[] value, int voffset,
      int vlength) throws IOException {
    return create(row, roffset, rlength, family, foffset, flength, qualifier, qoffset, qlength,
        timestamp, value, voffset, vlength, (List<Tag>)null);
  }

  /**
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @param visExpression visibility expression to be associated with cell
   * @return created Cell
   * @throws IOException
   * @deprecated since 0.98.9
   * @see <a href="https://issues.apache.org/jira/browse/HBASE-10560">HBASE-10560</a>
   */
  @Deprecated
  public Cell create(byte[] row, int roffset, int rlength, byte[] family, int foffset, int flength,
      byte[] qualifier, int qoffset, int qlength, long timestamp, byte[] value, int voffset,
      int vlength, String visExpression) throws IOException {
    List<Tag> visTags = null;
    if (visExpression != null) {
      visTags = this.visExpResolver.createVisibilityExpTags(visExpression);
    }
    return new KeyValue(row, roffset, rlength, family, foffset, flength, qualifier, qoffset,
        qlength, timestamp, KeyValue.Type.Put, value, voffset, vlength, visTags);
  }

  /**
   * @param row row key
   * @param roffset row offset
   * @param rlength row length
   * @param family family name
   * @param foffset family offset
   * @param flength family length
   * @param qualifier column qualifier
   * @param qoffset qualifier offset
   * @param qlength qualifier length
   * @param timestamp version timestamp
   * @param value column value
   * @param voffset value offset
   * @param vlength value length
   * @param tags
   * @return created Cell
   * @throws IOException
   */
  public Cell create(byte[] row, int roffset, int rlength, byte[] family, int foffset, int flength,
      byte[] qualifier, int qoffset, int qlength, long timestamp, byte[] value, int voffset,
      int vlength, List<Tag> tags) throws IOException {
    return new KeyValue(row, roffset, rlength, family, foffset, flength, qualifier, qoffset,
        qlength, timestamp, KeyValue.Type.Put, value, voffset, vlength, tags);
  }

  /**
   * @return Visibility expression resolver
   */
  public VisibilityExpressionResolver getVisibilityExpressionResolver() {
    return this.visExpResolver;
  }
}
