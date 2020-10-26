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
package org.apache.hadoop.hbase;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.ClientMetaTableAccessor.QueryType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.RegionInfoBuilder;
import org.apache.hadoop.hbase.client.RegionReplicaUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableState;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.PairOfSameType;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * NOTE: This class is currently empty as all it's contents is in MetaTableAccessor. The
 * reason behind doing this is to avoid including files with only class rename changes
 * (eg MetaTableAccessor -> CatalogAccessor) in the pull request when doing a review. If this
 * feature (HBASE-11288) gets merged as a second step we just move the contents into the class
 * and perform a search and replace for MetaTableAccessor -> CatalogAccessor.
 *
 * Read/write operations on <code>hbase:meta</code> region as well as assignment information stored
 * to <code>hbase:meta</code>.
 * <p/>
 * Some of the methods of this class take ZooKeeperWatcher as a param. The only reason for this is
 * when this class is used on client-side (e.g. HBaseAdmin), we want to use short-lived connection
 * (opened before each operation, closed right after), while when used on HM or HRS (like in
 * AssignmentManager) we want permanent connection.
 * <p/>
 * HBASE-10070 adds a replicaId to HRI, meaning more than one HRI can be defined for the same table
 * range (table, startKey, endKey). For every range, there will be at least one HRI defined which is
 * called default replica.
 * <p/>
 * <h2>Meta layout</h2> For each table there is single row named for the table with a 'table' column
 * family. The column family currently has one column in it, the 'state' column:
 *
 * <pre>
 * table:state => contains table state
 * </pre>
 *
 * For the catalog family, see the comments of {@link CatalogFamilyFormat} for more details.
 * <p/>
 * TODO: Add rep_barrier for serial replication explanation. See SerialReplicationChecker.
 * <p/>
 * The actual layout of meta should be encapsulated inside CatalogAccessor methods, and should not
 * leak out of it (through Result objects, etc)
 * @see CatalogFamilyFormat
 * @see ClientMetaTableAccessor
 */
@InterfaceAudience.Private
public class CatalogAccessor extends MetaTableAccessor {


}
