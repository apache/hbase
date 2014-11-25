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

import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_FAMILY;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABELS_TABLE_NAME;
import static org.apache.hadoop.hbase.security.visibility.VisibilityConstants.LABEL_QUALIFIER;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.security.visibility.Authorizations;
import org.apache.hadoop.hbase.security.visibility.VisibilityLabelOrdinalProvider;
import org.apache.hadoop.hbase.security.visibility.VisibilityUtils;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * This implementation creates tags by expanding expression using label ordinal. Labels will be
 * serialized in sorted order of it's ordinal.
 */
@InterfaceAudience.Private
public class DefaultVisibilityExpressionResolver implements VisibilityExpressionResolver {
  private static final Log LOG = LogFactory.getLog(DefaultVisibilityExpressionResolver.class);

  private Configuration conf;
  private final Map<String, Integer> labels = new HashMap<String, Integer>();

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public void init() {
    // Reading all the labels and ordinal.
    // This scan should be done by user with global_admin privileges.. Ensure that it works
    Table labelsTable = null;
    Connection connection = null;
    try {
      connection = ConnectionFactory.createConnection(conf);
      try {
        labelsTable = connection.getTable(LABELS_TABLE_NAME);
      } catch (TableNotFoundException e) {
        // Just return with out doing any thing. When the VC is not used we wont be having 'labels'
        // table in the cluster.
        return;
      } catch (IOException e) {
        LOG.error("Error opening 'labels' table", e);
        return;
      }
      Scan scan = new Scan();
      scan.setAuthorizations(new Authorizations(VisibilityUtils.SYSTEM_LABEL));
      scan.addColumn(LABELS_TABLE_FAMILY, LABEL_QUALIFIER);
      ResultScanner scanner = null;
      try {
        scanner = labelsTable.getScanner(scan);
        Result next = null;
        while ((next = scanner.next()) != null) {
          byte[] row = next.getRow();
          byte[] value = next.getValue(LABELS_TABLE_FAMILY, LABEL_QUALIFIER);
          labels.put(Bytes.toString(value), Bytes.toInt(row));
        }
      } catch (IOException e) {
        LOG.error("Error scanning 'labels' table", e);
      } finally {
        if (scanner != null) scanner.close();
      }
    } catch (IOException ioe) {
      LOG.error("Failed reading 'labels' tags", ioe);
      return;
    } finally {
      if (labelsTable != null) {
        try {
          labelsTable.close();
        } catch (IOException ioe) {
          LOG.warn("Error closing 'labels' table", ioe);
        }
      }
      if (connection != null)
        try {
          connection.close();
        } catch (IOException ioe) {
          LOG.warn("Failed close of temporary connection", ioe);
        }
    }
  }

  @Override
  public List<Tag> createVisibilityExpTags(String visExpression) throws IOException {
    VisibilityLabelOrdinalProvider provider = new VisibilityLabelOrdinalProvider() {
      @Override
      public int getLabelOrdinal(String label) {
        return labels.get(label);
      }
    };
    return VisibilityUtils.createVisibilityExpTags(visExpression, true, false, null, provider);
  }
}