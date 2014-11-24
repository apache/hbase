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

package org.apache.hadoop.hbase.quotas;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.protobuf.generated.QuotaProtos.Quotas;
import org.apache.hadoop.util.StringUtils;

/**
 * Scanner to iterate over the quota settings.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class QuotaRetriever implements Closeable, Iterable<QuotaSettings> {
  private static final Log LOG = LogFactory.getLog(QuotaRetriever.class);

  private final Queue<QuotaSettings> cache = new LinkedList<QuotaSettings>();
  private ResultScanner scanner;
  private HTable table;

  private QuotaRetriever() {
  }

  void init(final Configuration conf, final Scan scan) throws IOException {
    table = new HTable(conf, QuotaTableUtil.QUOTA_TABLE_NAME);
    try {
      scanner = table.getScanner(scan);
    } catch (IOException e) {
      table.close();
      throw e;
    }
  }

  public void close() throws IOException {
    table.close();
  }

  public QuotaSettings next() throws IOException {
    if (cache.isEmpty()) {
      Result result = scanner.next();
      if (result == null) return null;

      QuotaTableUtil.parseResult(result, new QuotaTableUtil.QuotasVisitor() {
        @Override
        public void visitUserQuotas(String userName, Quotas quotas) {
          cache.addAll(QuotaSettingsFactory.fromUserQuotas(userName, quotas));
        }

        @Override
        public void visitUserQuotas(String userName, TableName table, Quotas quotas) {
          cache.addAll(QuotaSettingsFactory.fromUserQuotas(userName, table, quotas));
        }

        @Override
        public void visitUserQuotas(String userName, String namespace, Quotas quotas) {
          cache.addAll(QuotaSettingsFactory.fromUserQuotas(userName, namespace, quotas));
        }

        @Override
        public void visitTableQuotas(TableName tableName, Quotas quotas) {
          cache.addAll(QuotaSettingsFactory.fromTableQuotas(tableName, quotas));
        }

        @Override
        public void visitNamespaceQuotas(String namespace, Quotas quotas) {
          cache.addAll(QuotaSettingsFactory.fromNamespaceQuotas(namespace, quotas));
        }
      });
    }
    return cache.poll();
  }

  @Override
  public Iterator<QuotaSettings> iterator() {
    return new Iter();
  }

  private class Iter implements Iterator<QuotaSettings> {
    QuotaSettings cache;

    public Iter() {
      try {
        cache = QuotaRetriever.this.next();
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
    }

    @Override
    public boolean hasNext() {
      return cache != null;
    }

    @Override
    public QuotaSettings next() {
      QuotaSettings result = cache;
      try {
        cache = QuotaRetriever.this.next();
      } catch (IOException e) {
        LOG.warn(StringUtils.stringifyException(e));
      }
      return result;
    }

    @Override
    public void remove() {
      throw new RuntimeException("remove() not supported");
    }
  }

  /**
   * Open a QuotaRetriever with no filter, all the quota settings will be returned.
   * @param conf Configuration object to use.
   * @return the QuotaRetriever
   * @throws IOException if a remote or network exception occurs
   */
  public static QuotaRetriever open(final Configuration conf) throws IOException {
    return open(conf, null);
  }

  /**
   * Open a QuotaRetriever with the specified filter.
   * @param conf Configuration object to use.
   * @param filter the QuotaFilter
   * @return the QuotaRetriever
   * @throws IOException if a remote or network exception occurs
   */
  public static QuotaRetriever open(final Configuration conf, final QuotaFilter filter)
      throws IOException {
    Scan scan = QuotaTableUtil.makeScan(filter);
    QuotaRetriever scanner = new QuotaRetriever();
    scanner.init(conf, scan);
    return scanner;
  }
}
