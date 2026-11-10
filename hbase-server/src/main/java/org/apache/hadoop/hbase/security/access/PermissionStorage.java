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

import com.google.errorprone.annotations.RestrictedApi;
import java.io.ByteArrayInputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.ExtendedCell;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
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

import org.apache.hadoop.hbase.shaded.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.shaded.protobuf.generated.AccessControlProtos;

/**
 * Maintains lists of permission grants to users and groups to allow for authorization checks by
 * {@link AccessController}.
 * <p>
 * Access control lists are stored in an "internal" metadata table named {@code _acl_}. Each table's
 * permission grants are stored as a separate row, keyed by the table name. KeyValues for
 * permissions assignments are stored in one of the formats:
 *
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
   * Delimiter to separate user, column family, and qualifier in _acl_ table info: column keys
   */
  public static final char ACL_KEY_DELIMITER = ',';

  private static final Logger LOG = LoggerFactory.getLogger(PermissionStorage.class);

  private PermissionStorage() {
  }

  private static byte[] toTableEntryName(TableName tableName) {
    return tableName.getName();
  }

  private static String getNamespaceFromEntryName(byte[] entryName) {
    return Bytes.toString(entryName, 1);
  }

  /**
   * Return the username for this table entry, and add family/qualifer to the given {@code builder}
   * if available.
   */
  private static String parseTableEntry(Permission.Builder builder, byte[] qual) {
    byte delimiter = (byte) ACL_KEY_DELIMITER;
    int idx = ArrayUtils.indexOf(qual, delimiter);
    // no family/qualifier
    if (idx < 0) {
      return Bytes.toString(qual);
    }
    int idx2 = ArrayUtils.indexOf(qual, delimiter, idx + 1);
    if (idx2 < 0) {
      // no qualifer
      builder.withFamily(Arrays.copyOfRange(qual, idx + 1, qual.length));
    } else {
      builder.withFamily(Arrays.copyOfRange(qual, idx + 1, idx2))
        .withQualifier(Arrays.copyOfRange(qual, idx2 + 1, qual.length));
    }
    return Bytes.toString(qual, 0, idx);
  }

  private static UserPermission parseUserPermission(byte[] entryName, byte[] qual,
    byte[] actionCodes) {
    if (actionCodes == null || actionCodes.length == 0) {
      return null;
    }
    String user;
    Permission.Builder builder;
    if (isGlobalEntry(entryName)) {
      builder = Permission.newBuilder();
      user = Bytes.toString(qual);
    } else if (isNamespaceEntry(entryName)) {
      builder = Permission.newBuilder(getNamespaceFromEntryName(entryName));
      user = Bytes.toString(qual);
    } else {
      // table entry
      builder = Permission.newBuilder(TableName.valueOf(entryName));
      user = parseTableEntry(builder, qual);
    }
    return new UserPermission(user, builder.withActionCodes(actionCodes).build());
  }

  // mainly used in grant/revoke, where we need to get the exact permission record for a given user
  private static UserPermission getUserPermission(Table aclTable, UserPermission userPerm)
    throws IOException {
    byte[] entryName = userPermissionRowKey(userPerm.getPermission());
    Get get = new Get(entryName);
    byte[] qual = userPermissionKey(userPerm);
    get.addColumn(ACL_LIST_FAMILY, qual);
    Result result = aclTable.get(get);
    if (result.isEmpty()) {
      return null;
    }
    byte[] actionCodes = result.getValue(ACL_LIST_FAMILY, qual);
    return parseUserPermission(entryName, qual, actionCodes);
  }

  private static byte[] getActionCodes(Permission perm) {
    Permission.Action[] actions = perm.getActions();
    byte[] actionCodes = new byte[actions.length];
    for (int i = 0; i < actionCodes.length; i++) {
      actionCodes[i] = actions[i].code();
    }
    return actionCodes;
  }

  private static void put(Table aclTable, UserPermission userPerm) throws IOException {
    Permission perm = userPerm.getPermission();
    Put put = new Put(userPermissionRowKey(perm));
    byte[] qual = userPermissionKey(userPerm);
    byte[] value = getActionCodes(perm);
    put.addColumn(ACL_LIST_FAMILY, qual, value);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing permission with rowKey " + Bytes.toString(put.getRow()) + " "
        + Bytes.toString(qual) + ": " + Bytes.toStringBinary(value));
    }
    aclTable.put(put);
  }

  private static void delete(UserPermission userPerm, Table aclTable) throws IOException {
    Delete d = new Delete(userPermissionRowKey(userPerm.getPermission()));
    byte[] qual = userPermissionKey(userPerm);
    d.addColumns(ACL_LIST_FAMILY, qual);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permission with rowKey " + Bytes.toString(d.getRow()) + " "
        + Bytes.toString(qual));
    }
    aclTable.delete(d);
  }

  /**
   * Stores a new user permission grant in the access control lists table.
   * @param conn     connection to hbase cluster
   * @param userPerm the details of the permission to be granted
   * @param aclTable acl table instance. It is closed upon method return.
   * @throws IOException in the case of an error accessing the metadata table
   */
  public static void addUserPermission(Connection conn, UserPermission userPerm,
    boolean mergeExistingPermissions) throws IOException {
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      UserPermission toAdd;
      if (mergeExistingPermissions) {
        UserPermission oldUserPerm = getUserPermission(aclTable, userPerm);
        if (oldUserPerm == null || ArrayUtils.isEmpty(oldUserPerm.getPermission().getActions())) {
          toAdd = userPerm;
        } else {
          Set<Permission.Action> actionSet = new TreeSet<>();
          actionSet.addAll(Arrays.asList(oldUserPerm.getPermission().getActions()));
          actionSet.addAll(Arrays.asList(userPerm.getPermission().getActions()));
          oldUserPerm.getPermission().setActions(actionSet.toArray(Permission.Action[]::new));
          toAdd = oldUserPerm;
        }
      } else {
        toAdd = userPerm;
      }
      put(aclTable, toAdd);
    }
  }

  /**
   * Removes a previously granted permission from the stored access control lists. The
   * {@link TablePermission} being removed must exactly match what is stored -- no wildcard matching
   * is attempted. Ie, if user "bob" has been granted "READ" access to the "data" table, but only to
   * column family plus qualifier "info:colA", then trying to call this method with only user "bob"
   * and the table name "data" (but without specifying the column qualifier "info:colA") will have
   * no effect.
   * @param conn     connection to hbase cluster
   * @param userPerm the details of the permission to be revoked
   * @throws IOException if there is an error accessing the metadata table
   */
  public static void removeUserPermission(Connection conn, UserPermission userPerm)
    throws IOException {
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      Permission perm = userPerm.getPermission();
      if (ArrayUtils.isEmpty(perm.getActions())) {
        delete(userPerm, aclTable);
        return;
      }
      UserPermission oldUserPerm = getUserPermission(aclTable, userPerm);
      if (oldUserPerm == null) {
        // nothing to revoke
        return;
      }
      Permission oldPerm = oldUserPerm.getPermission();
      Set<Permission.Action> remainingActions = new TreeSet<>();
      if (!ArrayUtils.isEmpty(oldPerm.getActions())) {
        remainingActions.addAll(Arrays.asList(oldPerm.getActions()));
      }
      for (Permission.Action action : perm.getActions()) {
        remainingActions.remove(action);
      }
      if (remainingActions.isEmpty()) {
        delete(userPerm, aclTable);
      } else {
        oldPerm.setActions(remainingActions.toArray(Permission.Action[]::new));
        put(aclTable, oldUserPerm);
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Removed permission " + userPerm.toString());
      }
    }
  }

  /**
   * Remove specified table from the _acl_ table.
   */
  static void removeTablePermissions(Connection conn, TableName tableName) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed table " + tableName);
    }
    Delete d = new Delete(toTableEntryName(tableName)).addFamily(ACL_LIST_FAMILY);
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      aclTable.delete(d);
    }
  }

  /**
   * Remove specified namespace from the acl table.
   */
  static void removeNamespacePermissions(Connection conn, String namespace) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed namespace " + namespace);
    }
    Delete d = new Delete(toNamespaceEntry(namespace)).addFamily(ACL_LIST_FAMILY);

    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      aclTable.delete(d);
    }
  }

  /**
   * Remove permissions of the specified column families of the given table from the acl table.
   */
  static void removeTableColumnFamiliesPermissions(Connection conn, TableName tableName,
    Collection<byte[]> columns) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed columns {} from table {}",
        columns.stream().map(Bytes::toStringBinary).collect(Collectors.joining(", ")), tableName);
    }
    Set<byte[]> families = new TreeSet<>(Bytes.BYTES_COMPARATOR);
    families.addAll(columns);
    List<UserPermission> userPerms = getTablePermissions(conn, tableName);
    Delete d = new Delete(toTableEntryName(tableName));
    boolean found = false;
    for (UserPermission userPerm : userPerms) {
      TablePermission perm = (TablePermission) userPerm.getPermission();
      byte[] family = perm.getFamily();
      if (family != null && families.contains(family)) {
        d.addColumns(ACL_LIST_FAMILY, userPermissionKey(userPerm));
        found = true;
      }
    }
    if (found) {
      try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
        aclTable.delete(d);
      }
    }
  }

  private static byte[] userPermissionRowKey(Permission permission) {
    byte[] row;
    if (permission instanceof TablePermission) {
      TablePermission tablePerm = (TablePermission) permission;
      row = toTableEntryName(tablePerm.getTableName());
    } else if (permission instanceof NamespacePermission) {
      NamespacePermission nsPerm = (NamespacePermission) permission;
      row = toNamespaceEntry(nsPerm.getNamespace());
    } else {
      // permission instanceof TablePermission
      row = ACL_GLOBAL_NAME;
    }
    return row;
  }

  /**
   * Build qualifier key from user permission: username username,family username,family,qualifier
   */
  private static byte[] userPermissionKey(UserPermission permission) {
    byte[] key = Bytes.toBytes(permission.getUser());
    byte[] qualifier = null;
    byte[] family = null;
    if (permission.getPermission().getAccessScope() == Permission.Scope.TABLE) {
      TablePermission tablePermission = (TablePermission) permission.getPermission();
      family = tablePermission.getFamily();
      qualifier = tablePermission.getQualifier();
    }

    if (family != null && family.length > 0) {
      key = Bytes.add(key, Bytes.add(new byte[] { ACL_KEY_DELIMITER }, family));
      if (qualifier != null && qualifier.length > 0) {
        key = Bytes.add(key, Bytes.add(new byte[] { ACL_KEY_DELIMITER }, qualifier));
      }
    }

    return key;
  }

  /**
   * Returns {@code true} if the given region is part of the {@code _acl_} metadata table.
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
   * Loads all of the permission grants stored in a region of the {@code _acl_} table.
   * @param aclRegion the acl region
   * @return a map of the permissions for this table.
   * @throws IOException if an error occurs
   */
  static Map<byte[], ListMultimap<String, UserPermission>> loadAll(Region aclRegion)
    throws IOException {
    if (!isAclRegion(aclRegion)) {
      throw new IOException("Can only load permissions from " + ACL_TABLE_NAME);
    }

    Map<byte[], ListMultimap<String, UserPermission>> allPerms =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

    // do a full scan of _acl_ table

    Scan scan = new Scan().addFamily(ACL_LIST_FAMILY);

    try (InternalScanner scanner = aclRegion.getScanner(scan);) {
      while (true) {
        List<Cell> row = new ArrayList<>();
        boolean hasNext = scanner.next(row);
        ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
        byte[] entry = null;
        for (Cell kv : row) {
          if (entry == null) {
            entry = CellUtil.cloneRow(kv);
          }
          UserPermission userPerm =
            parseUserPermission(entry, CellUtil.cloneQualifier(kv), CellUtil.cloneValue(kv));
          if (userPerm != null) {
            perms.put(userPerm.getUser(), userPerm);
          }
        }
        if (entry != null) {
          allPerms.put(entry, perms);
        }
        if (!hasNext) {
          break;
        }
      }
    }

    return allPerms;
  }

  /**
   * Load all permissions from the region server holding {@code _acl_}, primarily intended for
   * testing purposes.
   */
  @RestrictedApi(explanation = "Should only be called in tests", link = "",
      allowedOnPath = ".*/src/test/.*")
  static Map<byte[], ListMultimap<String, UserPermission>> loadAll(Connection conn)
    throws IOException {
    Map<byte[], ListMultimap<String, UserPermission>> allPerms =
      new TreeMap<>(Bytes.BYTES_RAWCOMPARATOR);

    // do a full scan of _acl_, filtering on only first table region rows

    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    // TODO: Pass in a Connection rather than create one each time.
    try (Table table = conn.getTable(ACL_TABLE_NAME);
      ResultScanner scanner = table.getScanner(scan)) {
      while (true) {
        Result result = scanner.next();
        if (result == null) {
          break;
        }
        byte[] entryName = result.getRow();
        ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
        result.getFamilyMap(ACL_LIST_FAMILY).forEach((qual, actionCodes) -> {
          UserPermission userPerm = parseUserPermission(entryName, qual, actionCodes);
          perms.put(userPerm.getUser(), userPerm);
        });
        allPerms.put(entryName, perms);
      }
    }

    return allPerms;
  }

  /**
   * Get all the permission entries for this table.
   */
  public static List<UserPermission> getTablePermissions(Connection conn, TableName tableName)
    throws IOException {
    byte[] entryName = toTableEntryName(tableName);
    Get get = new Get(entryName).addFamily(ACL_LIST_FAMILY);
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      Result result = aclTable.get(get);
      if (result.isEmpty()) {
        return Collections.emptyList();
      }
      List<UserPermission> userPerms = new ArrayList<>();
      result.getFamilyMap(ACL_LIST_FAMILY).forEach((qual, actionCodes) -> {
        Permission.Builder builder = Permission.newBuilder(tableName).withActionCodes(actionCodes);
        String user = parseTableEntry(builder, qual);
        userPerms.add(new UserPermission(user, builder.build()));
      });
      return userPerms;
    }
  }

  public static List<UserPermission> getNamespacePermissions(Connection conn, String namespace)
    throws IOException {
    byte[] entryName = toNamespaceEntry(namespace);
    Get get = new Get(entryName).addFamily(ACL_LIST_FAMILY);
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      Result result = aclTable.get(get);
      if (result.isEmpty()) {
        return Collections.emptyList();
      }
      List<UserPermission> userPerms = new ArrayList<>();
      result.getFamilyMap(ACL_LIST_FAMILY).forEach((qual, actionCodes) -> {
        Permission perm = Permission.newBuilder(namespace).withActionCodes(actionCodes).build();
        String user = Bytes.toString(qual);
        userPerms.add(new UserPermission(user, perm));
      });
      return userPerms;
    }
  }

  public static List<UserPermission> getGlobalPermissions(Connection conn) throws IOException {
    Get get = new Get(ACL_GLOBAL_NAME).addFamily(ACL_LIST_FAMILY);
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      Result result = aclTable.get(get);
      if (result.isEmpty()) {
        return Collections.emptyList();
      }
      List<UserPermission> userPerms = new ArrayList<>();
      result.getFamilyMap(ACL_LIST_FAMILY).forEach((qual, actionCodes) -> {
        Permission perm = Permission.newBuilder().withActionCodes(actionCodes).build();
        String user = Bytes.toString(qual);
        userPerms.add(new UserPermission(user, perm));
      });
      return userPerms;
    }
  }

  public static boolean isGlobalEntry(byte[] entryName) {
    return Bytes.equals(entryName, ACL_GLOBAL_NAME);
  }

  public static boolean isNamespaceEntry(String entryName) {
    return isNamespaceEntry(Bytes.toBytes(entryName));
  }

  public static boolean isNamespaceEntry(byte[] entryName) {
    return entryName != null && entryName.length != 0 && entryName[0] == NAMESPACE_PREFIX;
  }

  public static boolean isTableEntry(byte[] entryName) {
    return !isNamespaceEntry(entryName) && !isGlobalEntry(entryName) && entryName != null;
  }

  public static byte[] toNamespaceEntry(String namespace) {
    return Bytes.toBytes(NAMESPACE_PREFIX + namespace);
  }

  public static String fromNamespaceEntry(String namespace) {
    if (namespace.charAt(0) != NAMESPACE_PREFIX) {
      throw new IllegalArgumentException("Argument is not a valid namespace entry");
    }
    return namespace.substring(1);
  }

  public static byte[] toNamespaceEntry(byte[] namespace) {
    byte[] ret = new byte[namespace.length + 1];
    ret[0] = NAMESPACE_PREFIX;
    System.arraycopy(namespace, 0, ret, 1, namespace.length);
    return ret;
  }

  public static byte[] fromNamespaceEntry(byte[] namespace) {
    if (namespace[0] != NAMESPACE_PREFIX) {
      throw new IllegalArgumentException(
        "Argument is not a valid namespace entry: " + Bytes.toString(namespace));
    }
    return Arrays.copyOfRange(namespace, 1, namespace.length);
  }

  public static List<Permission> getCellPermissionsForUser(User user, ExtendedCell cell)
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
        ListMultimap<String, Permission> kvPerms =
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

  // ====================
  // Below are methods for sync permission between acl table and zk
  // ====================

  /**
   * Reads user permission assignments stored in the <code>l:</code> column family of the first
   * table row in <code>_acl_</code>.
   * <p>
   * See {@link PermissionStorage class documentation} for the key structure used for storage.
   * </p>
   */
  static ListMultimap<String, UserPermission> getPermissions(Connection conn, byte[] entryName)
    throws IOException {
    // for normal user tables, we just read the table row from _acl_
    ListMultimap<String, UserPermission> perms = ArrayListMultimap.create();
    Get get = new Get(entryName);
    get.addFamily(ACL_LIST_FAMILY);
    Result result;
    try (Table aclTable = conn.getTable(ACL_TABLE_NAME)) {
      result = aclTable.get(get);
    }
    if (!result.isEmpty()) {
      result.getFamilyMap(ACL_LIST_FAMILY).forEach((qual, actionCodes) -> {
        UserPermission userPerms = parseUserPermission(entryName, qual, actionCodes);
        perms.put(userPerms.getUser(), userPerms);
      });
    } else {
      LOG.info("No permissions found in " + ACL_TABLE_NAME + " for acl entry "
        + Bytes.toString(entryName));
    }

    return perms;
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

  private static List<Permission> readWritableUserPermission(DataInput in, Configuration conf)
    throws IOException, ClassNotFoundException {
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

  public static ListMultimap<String, Permission> readPermissions(byte[] data, Configuration conf)
    throws DeserializationException {
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
}
