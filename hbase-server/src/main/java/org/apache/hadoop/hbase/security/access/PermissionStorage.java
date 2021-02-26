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

package org.apache.hadoop.hbase.security.access;

import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.Cell.Type;
import org.apache.hadoop.hbase.CellBuilderFactory;
import org.apache.hadoop.hbase.CellBuilderType;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableFactories;
import org.apache.hadoop.io.WritableUtils;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hbase.thirdparty.com.google.common.collect.ArrayListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.ListMultimap;
import org.apache.hbase.thirdparty.com.google.common.collect.Lists;

/**
 * Maintains lists of permission grants to users and groups to allow for
 * authorization checks by {@link AccessController}.
 *
 * <p>
 * Access control lists are stored in an "internal" metadata table named
 * {@code _acl_}. Each table's permission grants are stored as a separate row,
 * keyed by the table name. KeyValues for permissions assignments are stored
 * in one of the formats:
 * <pre>
 * Key                      Desc
 * --------                 --------
 * user                     table level permissions for a user [R=read, W=write]
 * group                    table level permissions for a group
 * user,family              column family level permissions for a user
 * group,family             column family level permissions for a group
 * user,family,qualifier    column qualifier level permissions for a user
 * group,family,qualifier   column qualifier level permissions for a group
 * </pre>
 * <p>
 * All values are encoded as byte arrays containing the codes from the
 * org.apache.hadoop.hbase.security.access.TablePermission.Action enum.
 * </p>
 */
@InterfaceAudience.Private
public final class PermissionStorage {
  /** Internal storage table for access control lists */
  public static final TableName ACL_TABLE_NAME =
      TableName.valueOf(NamespaceDescriptor.SYSTEM_NAMESPACE_NAME_STR, "acl");
  public static final byte[] ACL_GLOBAL_NAME = ACL_TABLE_NAME.getName();
  /** Column family used to store ACL grants */
  public static final String ACL_LIST_FAMILY_STR = "l";
  public static final byte[] ACL_LIST_FAMILY = Bytes.toBytes(ACL_LIST_FAMILY_STR);
  /** KV tag to store per cell access control lists */
  public static final byte ACL_TAG_TYPE = TagType.ACL_TAG_TYPE;

  public static final char NAMESPACE_PREFIX = '@';

  /**
   * Delimiter to separate user, column family, and qualifier in
   * _acl_ table info: column keys */
  public static final char ACL_KEY_DELIMITER = ',';

  private static final Logger LOG = LoggerFactory.getLogger(PermissionStorage.class);

  private PermissionStorage() {
  }

  /**
   * Stores a new user permission grant in the access control lists table.
   * @param conf the configuration
   * @param userPerm the details of the permission to be granted
   * @param t acl table instance. It is closed upon method return.
   * @throws IOException in the case of an error accessing the metadata table
   */
  public static void addUserPermission(Configuration conf, UserPermission userPerm, Table t,
      boolean mergeExistingPermissions) throws IOException {
    Permission permission = userPerm.getPermission();
    Permission.Action[] actions = permission.getActions();
    byte[] rowKey = userPermissionRowKey(permission);
    Put p = new Put(rowKey);
    byte[] key = userPermissionKey(userPerm);

    if ((actions == null) || (actions.length == 0)) {
      String msg = "No actions associated with user '" + userPerm.getUser() + "'";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    Set<Permission.Action> actionSet = new TreeSet<Permission.Action>();
    if(mergeExistingPermissions){
      List<UserPermission> perms = getUserPermissions(conf, rowKey, null, null, null, false);
      UserPermission currentPerm = null;
      for (UserPermission perm : perms) {
        if (userPerm.equalsExceptActions(perm)) {
          currentPerm = perm;
          break;
        }
      }

      if (currentPerm != null && currentPerm.getPermission().getActions() != null){
        actionSet.addAll(Arrays.asList(currentPerm.getPermission().getActions()));
      }
    }

    // merge current action with new action.
    actionSet.addAll(Arrays.asList(actions));

    // serialize to byte array.
    byte[] value = new byte[actionSet.size()];
    int index = 0;
    for (Permission.Action action : actionSet) {
      value[index++] = action.code();
    }
    p.add(CellBuilderFactory.create(CellBuilderType.SHALLOW_COPY)
        .setRow(p.getRow())
        .setFamily(ACL_LIST_FAMILY)
        .setQualifier(key)
        .setTimestamp(p.getTimestamp())
        .setType(Type.Put)
        .setValue(value)
        .build());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing permission with rowKey " + Bytes.toString(rowKey) + " "
          + Bytes.toString(key) + ": " + Bytes.toStringBinary(value));
    }
    try {
      t.put(p);
    } finally {
      t.close();
    }
  }

  static void addUserPermission(Configuration conf, UserPermission userPerm, Table t)
          throws IOException{
    addUserPermission(conf, userPerm, t, false);
  }

  /**
   * Removes a previously granted permission from the stored access control
   * lists.  The {@link TablePermission} being removed must exactly match what
   * is stored -- no wildcard matching is attempted.  Ie, if user "bob" has
   * been granted "READ" access to the "data" table, but only to column family
   * plus qualifier "info:colA", then trying to call this method with only
   * user "bob" and the table name "data" (but without specifying the
   * column qualifier "info:colA") will have no effect.
   *
   * @param conf the configuration
   * @param userPerm the details of the permission to be revoked
   * @param t acl table
   * @throws IOException if there is an error accessing the metadata table
   */
  public static void removeUserPermission(Configuration conf, UserPermission userPerm, Table t)
      throws IOException {
    if (null == userPerm.getPermission().getActions() ||
        userPerm.getPermission().getActions().length == 0) {
      removePermissionRecord(conf, userPerm, t);
    } else {
      // Get all the global user permissions from the acl table
      List<UserPermission> permsList =
        getUserPermissions(conf, userPermissionRowKey(userPerm.getPermission()),
          null, null, null, false);
      List<Permission.Action> remainingActions = new ArrayList<>();
      List<Permission.Action> dropActions = Arrays.asList(userPerm.getPermission().getActions());
      for (UserPermission perm : permsList) {
        // Find the user and remove only the requested permissions
        if (perm.getUser().equals(userPerm.getUser())) {
          for (Permission.Action oldAction : perm.getPermission().getActions()) {
            if (!dropActions.contains(oldAction)) {
              remainingActions.add(oldAction);
            }
          }
          if (!remainingActions.isEmpty()) {
            perm.getPermission().setActions(
              remainingActions.toArray(new Permission.Action[remainingActions.size()]));
            addUserPermission(conf, perm, t);
          } else {
            removePermissionRecord(conf, userPerm, t);
          }
          break;
        }
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removed permission "+ userPerm.toString());
    }
  }

  private static void removePermissionRecord(Configuration conf, UserPermission userPerm, Table t)
      throws IOException {
    Delete d = new Delete(userPermissionRowKey(userPerm.getPermission()));
    d.addColumns(ACL_LIST_FAMILY, userPermissionKey(userPerm));
    try {
      t.delete(d);
    } finally {
      t.close();
    }
  }

  /**
   * Remove specified table from the _acl_ table.
   */
  static void removeTablePermissions(Configuration conf, TableName tableName, Table t)
      throws IOException{
    Delete d = new Delete(tableName.getName());
    d.addFamily(ACL_LIST_FAMILY);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed table "+ tableName);
    }
    try {
      t.delete(d);
    } finally {
      t.close();
    }
  }

  /**
   * Remove specified namespace from the acl table.
   */
  static void removeNamespacePermissions(Configuration conf, String namespace, Table t)
      throws IOException{
    Delete d = new Delete(Bytes.toBytes(toNamespaceEntry(namespace)));
    d.addFamily(ACL_LIST_FAMILY);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed namespace "+ namespace);
    }

    try {
      t.delete(d);
    } finally {
      t.close();
    }
  }

  static private void removeTablePermissions(TableName tableName, byte[] column, Table table,
      boolean closeTable) throws IOException {
    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    String columnName = Bytes.toString(column);
    scan.setFilter(new QualifierFilter(CompareOperator.EQUAL, new RegexStringComparator(
        String.format("(%s%s%s)|(%s%s)$",
            ACL_KEY_DELIMITER, columnName, ACL_KEY_DELIMITER,
            ACL_KEY_DELIMITER, columnName))));

    Set<byte[]> qualifierSet = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    ResultScanner scanner = null;
    try {
      scanner = table.getScanner(scan);
      for (Result res : scanner) {
        for (byte[] q : res.getFamilyMap(ACL_LIST_FAMILY).navigableKeySet()) {
          qualifierSet.add(q);
        }
      }

      if (qualifierSet.size() > 0) {
        Delete d = new Delete(tableName.getName());
        for (byte[] qualifier : qualifierSet) {
          d.addColumns(ACL_LIST_FAMILY, qualifier);
        }
        table.delete(d);
      }
    } finally {
      if (scanner != null) {
        scanner.close();
      }
      if (closeTable) {
        table.close();
      }
    }
  }

  /**
   * Remove specified table column from the acl table.
   */
  static void removeTablePermissions(Configuration conf, TableName tableName, byte[] column,
      Table t) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed column " + Bytes.toString(column) +
          " from table "+ tableName);
    }
    removeTablePermissions(tableName, column, t, true);
  }

  static byte[] userPermissionRowKey(Permission permission) {
    byte[] row;
    if (permission instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission) permission;
      row = tablePerm.getTableName().getName();
    } else if (permission instanceof NamespacePermission) {
      NamespacePermission nsPerm = (NamespacePermission) permission;
      row = Bytes.toBytes(toNamespaceEntry(nsPerm.getNamespace()));
    } else {
      // permission instanceof TablePermission
      row = ACL_GLOBAL_NAME;
    }
    return row;
  }

  /**
   * Build qualifier key from user permission:
   *  username
   *  username,family
   *  username,family,qualifier
   */
  static byte[] userPermissionKey(UserPermission permission) {
    byte[] key = Bytes.toBytes(permission.getUser());
    byte[] qualifier = null;
    byte[] family = null;
    if (permission.getPermission().getAccessScope() == Permission.Scope.TABLE) {
      TablePermission tablePermission = (TablePermission) permission.getPermission();
      family = tablePermission.getFamily();
      qualifier = tablePermission.getQualifier();
    }

    if (family != null && family.length > 0) {
      key = Bytes.add(key, Bytes.add(new byte[]{ACL_KEY_DELIMITER}, family));
      if (qualifier != null && qualifier.length > 0) {
        key = Bytes.add(key, Bytes.add(new byte[]{ACL_KEY_DELIMITER}, qualifier));
      }
    }

    return key;
  }

  /**
   * Returns {@code true} if the given region is part of the {@code _acl_}
   * metadata table.
   */
  static boolean isAclRegion(Region region) {
    return ACL_TABLE_NAME.equals(region.getTableDescriptor().getTableName());
  }

  /**
   * Returns {@code true} if the given table is {@code _acl_} metadata table.
   */
  static boolean isAclTable(TableDescriptor desc) {
    return ACL_TABLE_NAME.equals(desc.getTableName());
  }

  /**
   * Loads all of the permission grants stored in a region of the {@code _acl_}
   * table.
   *
   * @param aclRegion the acl region
   * @return a map of the permissions for this table.
   * @throws IOException if an error occurs
   */
  static Map<byte[], ListMultimap<String, UserPermission>> loadAll(Region aclRegion)
      throws IOException {
    if (!isAclRegion(aclRegion)) {
      throw new IOException("Can only load permissions from "+ACL_TABLE_NAME);
    }

    Map<byte[], ListMultimap<String, UserPermission>> allPerms =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

    // do a full scan of _acl_ table

    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    InternalScanner iScanner = null;
    try {
      iScanner = aclRegion.getScanner(scan);

      while (true) {
        List<Cell> row = new ArrayList<>();

        boolean hasNext = iScanner.next(row);
        ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
        byte[] entry = null;
        for (Cell kv : row) {
          if (entry == null) {
            entry = CellUtil.cloneRow(kv);
          }
          Pair<String, Permission> permissionsOfUserOnTable =
              parsePermissionRecord(entry, kv, null, null, false, null);
          if (permissionsOfUserOnTable != null) {
            String username = permissionsOfUserOnTable.getFirst();
            Permission permission = permissionsOfUserOnTable.getSecond();
            perms.put(username, new UserPermission(username, permission));
          }
        }
        if (entry != null) {
          allPerms.put(entry, perms);
        }
        if (!hasNext) {
          break;
        }
      }
    } finally {
      if (iScanner != null) {
        iScanner.close();
      }
    }

    return allPerms;
  }

  /**
   * Load all permissions from the region server holding {@code _acl_},
   * primarily intended for testing purposes.
   */
  static Map<byte[], ListMultimap<String, UserPermission>> loadAll(
      Configuration conf) throws IOException {
    Map<byte[], ListMultimap<String, UserPermission>> allPerms =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

    // do a full scan of _acl_, filtering on only first table region rows

    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    ResultScanner scanner = null;
    // TODO: Pass in a Connection rather than create one each time.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        scanner = table.getScanner(scan);
        try {
          for (Result row : scanner) {
            ListMultimap<String, UserPermission> resultPerms =
                parsePermissions(row.getRow(), row, null, null, null, false);
            allPerms.put(row.getRow(), resultPerms);
          }
        } finally {
          if (scanner != null) {
            scanner.close();
          }
        }
      }
    }

    return allPerms;
  }

  public static ListMultimap<String, UserPermission> getTablePermissions(Configuration conf,
      TableName tableName) throws IOException {
    return getPermissions(conf, tableName != null ? tableName.getName() : null, null, null, null,
      null, false);
  }

  public static ListMultimap<String, UserPermission> getNamespacePermissions(Configuration conf,
      String namespace) throws IOException {
    return getPermissions(conf, Bytes.toBytes(toNamespaceEntry(namespace)), null, null, null, null,
      false);
  }

  public static ListMultimap<String, UserPermission> getGlobalPermissions(Configuration conf)
      throws IOException {
    return getPermissions(conf, null, null, null, null, null, false);
  }

  /**
   * Reads user permission assignments stored in the <code>l:</code> column family of the first
   * table row in <code>_acl_</code>.
   * <p>
   * See {@link PermissionStorage class documentation} for the key structure used for storage.
   * </p>
   */
  static ListMultimap<String, UserPermission> getPermissions(Configuration conf, byte[] entryName,
      Table t, byte[] cf, byte[] cq, String user, boolean hasFilterUser) throws IOException {
    if (entryName == null) {
      entryName = ACL_GLOBAL_NAME;
    }
    // for normal user tables, we just read the table row from _acl_
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    Get get = new Get(entryName);
    get.addFamily(ACL_LIST_FAMILY);
    Result row = null;
    if (t == null) {
      try (Connection connection = ConnectionFactory.createConnection(conf)) {
        try (Table table = connection.getTable(ACL_TABLE_NAME)) {
          row = table.get(get);
        }
      }
    } else {
      row = t.get(get);
    }
    if (!row.isEmpty()) {
      perms = parsePermissions(entryName, row, cf, cq, user, hasFilterUser);
    } else {
      LOG.info("No permissions found in " + ACL_TABLE_NAME + " for acl entry "
          + Bytes.toString(entryName));
    }

    return perms;
  }

  /**
   * Returns the currently granted permissions for a given table as the specified user plus
   * associated permissions.
   */
  public static List<UserPermission> getUserTablePermissions(Configuration conf,
      TableName tableName, byte[] cf, byte[] cq, String userName, boolean hasFilterUser)
      throws IOException {
    return getUserPermissions(conf, tableName == null ? null : tableName.getName(), cf, cq,
      userName, hasFilterUser);
  }

  /**
   * Returns the currently granted permissions for a given namespace as the specified user plus
   * associated permissions.
   */
  public static List<UserPermission> getUserNamespacePermissions(Configuration conf,
      String namespace, String user, boolean hasFilterUser) throws IOException {
    return getUserPermissions(conf, Bytes.toBytes(toNamespaceEntry(namespace)), null, null, user,
      hasFilterUser);
  }

  /**
   * Returns the currently granted permissions for a given table/namespace with associated
   * permissions based on the specified column family, column qualifier and user name.
   * @param conf the configuration
   * @param entryName Table name or the namespace
   * @param cf Column family
   * @param cq Column qualifier
   * @param user User name to be filtered from permission as requested
   * @param hasFilterUser true if filter user is provided, otherwise false.
   * @return List of UserPermissions
   * @throws IOException on failure
   */
  public static List<UserPermission> getUserPermissions(Configuration conf, byte[] entryName,
      byte[] cf, byte[] cq, String user, boolean hasFilterUser) throws IOException {
    ListMultimap<String, UserPermission> allPerms =
        getPermissions(conf, entryName, null, cf, cq, user, hasFilterUser);
    List<UserPermission> perms = new ArrayList<>();
    for (Map.Entry<String, UserPermission> entry : allPerms.entries()) {
      perms.add(entry.getValue());
    }
    return perms;
  }

  /**
   * Parse and filter permission based on the specified column family, column qualifier and user
   * name.
   */
  private static ListMultimap<String, UserPermission> parsePermissions(byte[] entryName,
      Result result, byte[] cf, byte[] cq, String user, boolean hasFilterUser) {
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    if (result != null && result.size() > 0) {
      for (Cell kv : result.rawCells()) {
        Pair<String, Permission> permissionsOfUserOnTable =
            parsePermissionRecord(entryName, kv, cf, cq, hasFilterUser, user);

        if (permissionsOfUserOnTable != null) {
          String username = permissionsOfUserOnTable.getFirst();
          Permission permission = permissionsOfUserOnTable.getSecond();
          perms.put(username, new UserPermission(username, permission));
        }
      }
    }
    return perms;
  }

  private static Pair<String, Permission> parsePermissionRecord(byte[] entryName, Cell kv,
      byte[] cf, byte[] cq, boolean filterPerms, String filterUser) {
    // return X given a set of permissions encoded in the permissionRecord kv.
    byte[] family = CellUtil.cloneFamily(kv);
    if (!Bytes.equals(family, ACL_LIST_FAMILY)) {
      return null;
    }

    byte[] key = CellUtil.cloneQualifier(kv);
    byte[] value = CellUtil.cloneValue(kv);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read acl: entry[" +
        Bytes.toStringBinary(entryName) + "], kv [" +
        Bytes.toStringBinary(key) + ": " +
        Bytes.toStringBinary(value)+"]");
    }

    // check for a column family appended to the key
    // TODO: avoid the string conversion to make this more efficient
    String username = Bytes.toString(key);

    // Retrieve group list for the filterUser if cell key is a group.
    // Group list is not required when filterUser itself a group
    List<String> filterUserGroups = null;
    if (filterPerms) {
      if (username.charAt(0) == '@' && !StringUtils.isEmpty(filterUser)
          && filterUser.charAt(0) != '@') {
        filterUserGroups = AccessChecker.getUserGroups(filterUser);
      }
    }

    // Handle namespace entry
    if (isNamespaceEntry(entryName)) {
      // Filter the permissions cell record if client query
      if (filterPerms && !validateFilterUser(username, filterUser, filterUserGroups)) {
        return null;
      }

      return new Pair<>(username,
          Permission.newBuilder(Bytes.toString(fromNamespaceEntry(entryName)))
              .withActionCodes(value).build());
    }

    // Handle global entry
    if (isGlobalEntry(entryName)) {
      // Filter the permissions cell record if client query
      if (filterPerms && !validateFilterUser(username, filterUser, filterUserGroups)) {
        return null;
      }

      return new Pair<>(username, Permission.newBuilder().withActionCodes(value).build());
    }

    // Handle table entry
    int idx = username.indexOf(ACL_KEY_DELIMITER);
    byte[] permFamily = null;
    byte[] permQualifier = null;
    if (idx > 0 && idx < username.length()-1) {
      String remainder = username.substring(idx+1);
      username = username.substring(0, idx);
      idx = remainder.indexOf(ACL_KEY_DELIMITER);
      if (idx > 0 && idx < remainder.length()-1) {
        permFamily = Bytes.toBytes(remainder.substring(0, idx));
        permQualifier = Bytes.toBytes(remainder.substring(idx+1));
      } else {
        permFamily = Bytes.toBytes(remainder);
      }
    }

    // Filter the permissions cell record if client query
    if (filterPerms) {
      // ACL table contain 3 types of cell key entries; hbase:Acl, namespace and table. So to filter
      // the permission cell records additional validations are required at CF, CQ and username.
      // Here we can proceed based on client input whether it contain filterUser.
      // Validate the filterUser when specified
      if (filterUser != null && !validateFilterUser(username, filterUser, filterUserGroups)) {
        return null;
      }
      if (!validateCFAndCQ(permFamily, cf, permQualifier, cq)) {
        return null;
      }
    }

    return new Pair<>(username, Permission.newBuilder(TableName.valueOf(entryName))
        .withFamily(permFamily).withQualifier(permQualifier).withActionCodes(value).build());
  }

  /*
   * Validate the cell key with the client filterUser if specified in the query input. 1. If cell
   * key (username) is not a group then check whether client filterUser is equal to username 2. If
   * cell key (username) is a group then check whether client filterUser belongs to the cell key
   * group (username) 3. In case when both filterUser and username are group names then cell will be
   * filtered if not equal.
   */
  private static boolean validateFilterUser(String username, String filterUser,
      List<String> filterUserGroups) {
    if (filterUserGroups == null) {
      // Validate user name or group names whether equal
      if (filterUser.equals(username)) {
        return true;
      }
    } else {
      // Check whether filter user belongs to the cell key group.
      return filterUserGroups.contains(username.substring(1));
    }
    return false;
  }

  /*
   * Validate the cell with client CF and CQ if specified in the query input. 1. If CF is NULL, then
   * no need of further validation, result should include all CF and CQ. 2. IF CF specified and
   * equal then validation required at CQ level if CF specified in client input, otherwise return
   * all CQ records.
   */
  private static boolean validateCFAndCQ(byte[] permFamily, byte[] cf, byte[] permQualifier,
      byte[] cq) {
    boolean include = true;
    if (cf != null) {
      if (Bytes.equals(cf, permFamily)) {
        if (cq != null && !Bytes.equals(cq, permQualifier)) {
          // if CQ specified and didn't match then ignore this cell
          include = false;
        }
      } else {
        // if CF specified and didn't match then ignore this cell
        include = false;
      }
    }
    return include;
  }

  /**
   * Writes a set of permissions as {@link org.apache.hadoop.io.Writable} instances and returns the
   * resulting byte array. Writes a set of permission [user: table permission]
   */
  public static byte[] writePermissionsAsBytes(ListMultimap<String, UserPermission> perms,
      Configuration conf) {
    return ProtobufUtil
        .prependPBMagic(AccessControlUtil.toUserTablePermissions(perms).toByteArray());
  }

  // This is part of the old HbaseObjectWritableFor96Migration.
  private static final int LIST_CODE = 61;

  private static final int WRITABLE_CODE = 14;

  private static final int WRITABLE_NOT_ENCODED = 0;

  private static List<Permission> readWritableUserPermission(DataInput in,
      Configuration conf) throws IOException, ClassNotFoundException {
    assert WritableUtils.readVInt(in) == LIST_CODE;
    int length = in.readInt();
    List<Permission> list = new ArrayList<>(length);
    for (int i = 0; i < length; i++) {
      assert WritableUtils.readVInt(in) == WRITABLE_CODE;
      assert WritableUtils.readVInt(in) == WRITABLE_NOT_ENCODED;
      String className = Text.readString(in);
      Class<? extends Writable> clazz = conf.getClassByName(className).asSubclass(Writable.class);
      Writable instance = WritableFactories.newInstance(clazz, conf);
      instance.readFields(in);
      list.add((Permission) instance);
    }
    return list;
  }

  public static ListMultimap<String, UserPermission> readUserPermission(byte[] data,
      Configuration conf) throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        AccessControlProtos.UsersAndPermissions.Builder builder =
          AccessControlProtos.UsersAndPermissions.newBuilder();
        ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
        return AccessControlUtil.toUserPermission(builder.build());
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    } else {
      // TODO: We have to re-write non-PB data as PB encoded. Otherwise we will carry old Writables
      // forever (here and a couple of other places).
      ListMultimap<String, UserPermission> userPermission = ArrayListMultimap.create();
      try {
        DataInput in = new DataInputStream(new ByteArrayInputStream(data));
        int length = in.readInt();
        for (int i = 0; i < length; i++) {
          String user = Text.readString(in);
          List<Permission> perms = readWritableUserPermission(in, conf);
          for (Permission p : perms) {
            userPermission.put(user, new UserPermission(user, p));
          }
        }
      } catch (IOException | ClassNotFoundException e) {
        throw new DeserializationException(e);
      }
      return userPermission;
    }
  }

  public static ListMultimap<String, Permission> readPermissions(byte[] data,
      Configuration conf) throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        AccessControlProtos.UsersAndPermissions.Builder builder =
          AccessControlProtos.UsersAndPermissions.newBuilder();
        ProtobufUtil.mergeFrom(builder, data, pblen, data.length - pblen);
        return AccessControlUtil.toPermission(builder.build());
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
    } else {
      // TODO: We have to re-write non-PB data as PB encoded. Otherwise we will carry old Writables
      // forever (here and a couple of other places).
      ListMultimap<String, Permission> perms = ArrayListMultimap.create();
      try {
        DataInput in = new DataInputStream(new ByteArrayInputStream(data));
        int length = in.readInt();
        for (int i = 0; i < length; i++) {
          String user = Text.readString(in);
          perms.putAll(user, readWritableUserPermission(in, conf));
        }
      } catch (IOException | ClassNotFoundException e) {
        throw new DeserializationException(e);
      }
      return perms;
    }
  }

  public static boolean isGlobalEntry(byte[] entryName) {
    return Bytes.equals(entryName, ACL_GLOBAL_NAME);
  }

  public static boolean isNamespaceEntry(String entryName) {
    return isNamespaceEntry(Bytes.toBytes(entryName));
  }

  public static boolean isNamespaceEntry(byte[] entryName) {
    return entryName != null && entryName.length !=0 && entryName[0] == NAMESPACE_PREFIX;
  }

  public static boolean isTableEntry(byte[] entryName) {
    return !isNamespaceEntry(entryName) && !isGlobalEntry(entryName) && entryName != null;
  }

  public static String toNamespaceEntry(String namespace) {
    return NAMESPACE_PREFIX + namespace;
  }

  public static String fromNamespaceEntry(String namespace) {
    if (namespace.charAt(0) != NAMESPACE_PREFIX) {
      throw new IllegalArgumentException("Argument is not a valid namespace entry");
    }
    return namespace.substring(1);
  }

  public static byte[] toNamespaceEntry(byte[] namespace) {
    byte[] ret = new byte[namespace.length+1];
    ret[0] = NAMESPACE_PREFIX;
    System.arraycopy(namespace, 0, ret, 1, namespace.length);
    return ret;
  }

  public static byte[] fromNamespaceEntry(byte[] namespace) {
    if(namespace[0] != NAMESPACE_PREFIX) {
      throw new IllegalArgumentException("Argument is not a valid namespace entry: " +
          Bytes.toString(namespace));
    }
    return Arrays.copyOfRange(namespace, 1, namespace.length);
  }

  public static List<Permission> getCellPermissionsForUser(User user, Cell cell)
      throws IOException {
    // Save an object allocation where we can
    if (cell.getTagsLength() == 0) {
      return null;
    }
    List<Permission> results = Lists.newArrayList();
    Iterator<Tag> tagsIterator = PrivateCellUtil.tagsIterator(cell);
    while (tagsIterator.hasNext()) {
      Tag tag = tagsIterator.next();
      if (tag.getType() == ACL_TAG_TYPE) {
        // Deserialize the table permissions from the KV
        // TODO: This can be improved. Don't build UsersAndPermissions just to unpack it again,
        // use the builder
        AccessControlProtos.UsersAndPermissions.Builder builder =
            AccessControlProtos.UsersAndPermissions.newBuilder();
        if (tag.hasArray()) {
          ProtobufUtil.mergeFrom(builder, tag.getValueArray(), tag.getValueOffset(),
            tag.getValueLength());
        } else {
          ProtobufUtil.mergeFrom(builder, Tag.cloneValue(tag));
        }
        ListMultimap<String,Permission> kvPerms =
            AccessControlUtil.toUsersAndPermissions(builder.build());
        // Are there permissions for this user?
        List<Permission> userPerms = kvPerms.get(user.getShortName());
        if (userPerms != null) {
          results.addAll(userPerms);
        }
        // Are there permissions for any of the groups this user belongs to?
        String[] groupNames = user.getGroupNames();
        if (groupNames != null) {
          for (String group : groupNames) {
            List<Permission> groupPerms = kvPerms.get(AuthUtil.toGroupEntry(group));
            if (results != null) {
              results.addAll(groupPerms);
            }
          }
        }
      }
    }
    return results;
  }
}
