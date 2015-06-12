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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.AuthUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.master.MasterServices;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.AccessControlProtos;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.Text;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.protobuf.InvalidProtocolBufferException;

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
 * All values are encoded as byte arrays containing the codes from the
 * org.apache.hadoop.hbase.security.access.TablePermission.Action enum.
 * </p>
 */
@InterfaceAudience.Private
public class AccessControlLists {
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

  private static final Log LOG = LogFactory.getLog(AccessControlLists.class);

  /**
   * Create the ACL table
   * @param master
   * @throws IOException
   */
  static void createACLTable(MasterServices master) throws IOException {
    master.createTable(new HTableDescriptor(ACL_TABLE_NAME)
      .addFamily(new HColumnDescriptor(ACL_LIST_FAMILY)
        .setMaxVersions(1)
        .setInMemory(true)
        .setBlockCacheEnabled(true)
        .setBlocksize(8 * 1024)
        .setBloomFilterType(BloomType.NONE)
        .setScope(HConstants.REPLICATION_SCOPE_LOCAL)
        // Set cache data blocks in L1 if more than one cache tier deployed; e.g. this will
        // be the case if we are using CombinedBlockCache (Bucket Cache).
        .setCacheDataInL1(true)),
    null);
  }

  /**
   * Stores a new user permission grant in the access control lists table.
   * @param conf the configuration
   * @param userPerm the details of the permission to be granted
   * @throws IOException in the case of an error accessing the metadata table
   */
  static void addUserPermission(Configuration conf, UserPermission userPerm)
      throws IOException {
    Permission.Action[] actions = userPerm.getActions();
    byte[] rowKey = userPermissionRowKey(userPerm);
    Put p = new Put(rowKey);
    byte[] key = userPermissionKey(userPerm);

    if ((actions == null) || (actions.length == 0)) {
      String msg = "No actions associated with user '" + Bytes.toString(userPerm.getUser()) + "'";
      LOG.warn(msg);
      throw new IOException(msg);
    }

    byte[] value = new byte[actions.length];
    for (int i = 0; i < actions.length; i++) {
      value[i] = actions[i].code();
    }
    p.addImmutable(ACL_LIST_FAMILY, key, value);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing permission with rowKey "+
          Bytes.toString(rowKey)+" "+
          Bytes.toString(key)+": "+Bytes.toStringBinary(value)
      );
    }
    // TODO: Pass in a Connection rather than create one each time.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        table.put(p);
      }
    }
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
   * @throws IOException if there is an error accessing the metadata table
   */
  static void removeUserPermission(Configuration conf, UserPermission userPerm)
      throws IOException {
    Delete d = new Delete(userPermissionRowKey(userPerm));
    byte[] key = userPermissionKey(userPerm);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permission "+ userPerm.toString());
    }
    d.addColumns(ACL_LIST_FAMILY, key);
    // TODO: Pass in a Connection rather than create one each time.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        table.delete(d);
      }
    }
  }

  /**
   * Remove specified table from the _acl_ table.
   */
  static void removeTablePermissions(Configuration conf, TableName tableName)
      throws IOException{
    Delete d = new Delete(tableName.getName());

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed table "+ tableName);
    }
    // TODO: Pass in a Connection rather than create one each time.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        table.delete(d);
      }
    }
  }

  /**
   * Remove specified namespace from the acl table.
   */
  static void removeNamespacePermissions(Configuration conf, String namespace)
      throws IOException{
    Delete d = new Delete(Bytes.toBytes(toNamespaceEntry(namespace)));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed namespace "+ namespace);
    }

    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        table.delete(d);
      }
    }
  }

  /**
   * Remove specified table column from the acl table.
   */
  static void removeTablePermissions(Configuration conf, TableName tableName, byte[] column)
      throws IOException{

    if (LOG.isDebugEnabled()) {
      LOG.debug("Removing permissions of removed column " + Bytes.toString(column) +
                " from table "+ tableName);
    }
    // TODO: Pass in a Connection rather than create one each time.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        Scan scan = new Scan();
        scan.addFamily(ACL_LIST_FAMILY);

        String columnName = Bytes.toString(column);
        scan.setFilter(new QualifierFilter(CompareOp.EQUAL, new RegexStringComparator(
            String.format("(%s%s%s)|(%s%s)$",
                ACL_KEY_DELIMITER, columnName, ACL_KEY_DELIMITER,
                ACL_KEY_DELIMITER, columnName))));

        Set<byte[]> qualifierSet = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        ResultScanner scanner = table.getScanner(scan);
        try {
          for (Result res : scanner) {
            for (byte[] q : res.getFamilyMap(ACL_LIST_FAMILY).navigableKeySet()) {
              qualifierSet.add(q);
            }
          }
        } finally {
          scanner.close();
        }

        if (qualifierSet.size() > 0) {
          Delete d = new Delete(tableName.getName());
          for (byte[] qualifier : qualifierSet) {
            d.addColumns(ACL_LIST_FAMILY, qualifier);
          }
          table.delete(d);
        }
      }
    }
  }

  static byte[] userPermissionRowKey(UserPermission userPerm) {
    byte[] row;
    if(userPerm.hasNamespace()) {
      row = Bytes.toBytes(toNamespaceEntry(userPerm.getNamespace()));
    } else if(userPerm.isGlobal()) {
      row = ACL_GLOBAL_NAME;
    } else {
      row = userPerm.getTableName().getName();
    }
    return row;
  }

  /**
   * Build qualifier key from user permission:
   *  username
   *  username,family
   *  username,family,qualifier
   */
  static byte[] userPermissionKey(UserPermission userPerm) {
    byte[] qualifier = userPerm.getQualifier();
    byte[] family = userPerm.getFamily();
    byte[] key = userPerm.getUser();

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
    return ACL_TABLE_NAME.equals(region.getTableDesc().getTableName());
  }

  /**
   * Returns {@code true} if the given table is {@code _acl_} metadata table.
   */
  static boolean isAclTable(HTableDescriptor desc) {
    return ACL_TABLE_NAME.equals(desc.getTableName());
  }

  /**
   * Loads all of the permission grants stored in a region of the {@code _acl_}
   * table.
   *
   * @param aclRegion
   * @return a map of the permissions for this table.
   * @throws IOException
   */
  static Map<byte[], ListMultimap<String,TablePermission>> loadAll(Region aclRegion)
    throws IOException {

    if (!isAclRegion(aclRegion)) {
      throw new IOException("Can only load permissions from "+ACL_TABLE_NAME);
    }

    Map<byte[], ListMultimap<String, TablePermission>> allPerms =
        new TreeMap<byte[], ListMultimap<String, TablePermission>>(Bytes.BYTES_RAWCOMPARATOR);

    // do a full scan of _acl_ table

    Scan scan = new Scan();
    scan.addFamily(ACL_LIST_FAMILY);

    InternalScanner iScanner = null;
    try {
      iScanner = aclRegion.getScanner(scan);

      while (true) {
        List<Cell> row = new ArrayList<Cell>();

        boolean hasNext = iScanner.next(row);
        ListMultimap<String,TablePermission> perms = ArrayListMultimap.create();
        byte[] entry = null;
        for (Cell kv : row) {
          if (entry == null) {
            entry = CellUtil.cloneRow(kv);
          }
          Pair<String,TablePermission> permissionsOfUserOnTable =
              parsePermissionRecord(entry, kv);
          if (permissionsOfUserOnTable != null) {
            String username = permissionsOfUserOnTable.getFirst();
            TablePermission permissions = permissionsOfUserOnTable.getSecond();
            perms.put(username, permissions);
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
  static Map<byte[], ListMultimap<String,TablePermission>> loadAll(
      Configuration conf) throws IOException {
    Map<byte[], ListMultimap<String,TablePermission>> allPerms =
        new TreeMap<byte[], ListMultimap<String,TablePermission>>(Bytes.BYTES_RAWCOMPARATOR);

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
            ListMultimap<String,TablePermission> resultPerms = parsePermissions(row.getRow(), row);
            allPerms.put(row.getRow(), resultPerms);
          }
        } finally {
          if (scanner != null) scanner.close();
        }
      }
    }

    return allPerms;
  }

  static ListMultimap<String, TablePermission> getTablePermissions(Configuration conf,
        TableName tableName) throws IOException {
    return getPermissions(conf, tableName != null ? tableName.getName() : null);
  }

  static ListMultimap<String, TablePermission> getNamespacePermissions(Configuration conf,
        String namespace) throws IOException {
    return getPermissions(conf, Bytes.toBytes(toNamespaceEntry(namespace)));
  }

  /**
   * Reads user permission assignments stored in the <code>l:</code> column
   * family of the first table row in <code>_acl_</code>.
   *
   * <p>
   * See {@link AccessControlLists class documentation} for the key structure
   * used for storage.
   * </p>
   */
  static ListMultimap<String, TablePermission> getPermissions(Configuration conf,
      byte[] entryName) throws IOException {
    if (entryName == null) entryName = ACL_GLOBAL_NAME;

    // for normal user tables, we just read the table row from _acl_
    ListMultimap<String, TablePermission> perms = ArrayListMultimap.create();
    // TODO: Pass in a Connection rather than create one each time.
    try (Connection connection = ConnectionFactory.createConnection(conf)) {
      try (Table table = connection.getTable(ACL_TABLE_NAME)) {
        Get get = new Get(entryName);
        get.addFamily(ACL_LIST_FAMILY);
        Result row = table.get(get);
        if (!row.isEmpty()) {
          perms = parsePermissions(entryName, row);
        } else {
          LOG.info("No permissions found in " + ACL_TABLE_NAME + " for acl entry "
              + Bytes.toString(entryName));
        }
      }
    }

    return perms;
  }

  /**
   * Returns the currently granted permissions for a given table as a list of
   * user plus associated permissions.
   */
  static List<UserPermission> getUserTablePermissions(
      Configuration conf, TableName tableName) throws IOException {
    return getUserPermissions(conf, tableName == null ? null : tableName.getName());
  }

  static List<UserPermission> getUserNamespacePermissions(
      Configuration conf, String namespace) throws IOException {
    return getUserPermissions(conf, Bytes.toBytes(toNamespaceEntry(namespace)));
  }

  static List<UserPermission> getUserPermissions(
      Configuration conf, byte[] entryName)
  throws IOException {
    ListMultimap<String,TablePermission> allPerms = getPermissions(
      conf, entryName);

    List<UserPermission> perms = new ArrayList<UserPermission>();

    for (Map.Entry<String, TablePermission> entry : allPerms.entries()) {
      UserPermission up = new UserPermission(Bytes.toBytes(entry.getKey()),
          entry.getValue().getTableName(), entry.getValue().getFamily(),
          entry.getValue().getQualifier(), entry.getValue().getActions());
      perms.add(up);
    }
    return perms;
  }

  private static ListMultimap<String, TablePermission> parsePermissions(
      byte[] entryName, Result result) {
    ListMultimap<String, TablePermission> perms = ArrayListMultimap.create();
    if (result != null && result.size() > 0) {
      for (Cell kv : result.rawCells()) {

        Pair<String,TablePermission> permissionsOfUserOnTable =
            parsePermissionRecord(entryName, kv);

        if (permissionsOfUserOnTable != null) {
          String username = permissionsOfUserOnTable.getFirst();
          TablePermission permissions = permissionsOfUserOnTable.getSecond();
          perms.put(username, permissions);
        }
      }
    }
    return perms;
  }

  private static Pair<String, TablePermission> parsePermissionRecord(
      byte[] entryName, Cell kv) {
    // return X given a set of permissions encoded in the permissionRecord kv.
    byte[] family = CellUtil.cloneFamily(kv);

    if (!Bytes.equals(family, ACL_LIST_FAMILY)) {
      return null;
    }

    byte[] key = CellUtil.cloneQualifier(kv);
    byte[] value = CellUtil.cloneValue(kv);
    if (LOG.isDebugEnabled()) {
      LOG.debug("Read acl: kv ["+
                Bytes.toStringBinary(key)+": "+
                Bytes.toStringBinary(value)+"]");
    }

    // check for a column family appended to the key
    // TODO: avoid the string conversion to make this more efficient
    String username = Bytes.toString(key);

    //Handle namespace entry
    if(isNamespaceEntry(entryName)) {
      return new Pair<String, TablePermission>(username,
          new TablePermission(Bytes.toString(fromNamespaceEntry(entryName)), value));
    }

    //Handle table and global entry
    //TODO global entry should be handled differently
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

    return new Pair<String,TablePermission>(username,
        new TablePermission(TableName.valueOf(entryName), permFamily, permQualifier, value));
  }

  /**
   * Writes a set of permissions as {@link org.apache.hadoop.io.Writable} instances
   * and returns the resulting byte array.
   *
   * Writes a set of permission [user: table permission]
   */
  public static byte[] writePermissionsAsBytes(ListMultimap<String, TablePermission> perms,
      Configuration conf) {
    return ProtobufUtil.prependPBMagic(ProtobufUtil.toUserTablePermissions(perms).toByteArray());
  }

  /**
   * Reads a set of permissions as {@link org.apache.hadoop.io.Writable} instances
   * from the input stream.
   */
  public static ListMultimap<String, TablePermission> readPermissions(byte[] data,
      Configuration conf)
  throws DeserializationException {
    if (ProtobufUtil.isPBMagicPrefix(data)) {
      int pblen = ProtobufUtil.lengthOfPBMagic();
      try {
        AccessControlProtos.UsersAndPermissions perms =
          AccessControlProtos.UsersAndPermissions.newBuilder().mergeFrom(
            data, pblen, data.length - pblen).build();
        return ProtobufUtil.toUserTablePermissions(perms);
      } catch (InvalidProtocolBufferException e) {
        throw new DeserializationException(e);
      }
    } else {
      ListMultimap<String,TablePermission> perms = ArrayListMultimap.create();
      try {
        DataInput in = new DataInputStream(new ByteArrayInputStream(data));
        int length = in.readInt();
        for (int i=0; i<length; i++) {
          String user = Text.readString(in);
          List<TablePermission> userPerms =
            (List)HbaseObjectWritableFor96Migration.readObject(in, conf);
          perms.putAll(user, userPerms);
        }
      } catch (IOException e) {
        throw new DeserializationException(e);
      }
      return perms;
    }
  }

  public static boolean isNamespaceEntry(String entryName) {
    return entryName.charAt(0) == NAMESPACE_PREFIX;
  }

  public static boolean isNamespaceEntry(byte[] entryName) {
    return entryName[0] == NAMESPACE_PREFIX;
  }

  public static String toNamespaceEntry(String namespace) {
     return NAMESPACE_PREFIX + namespace;
   }

   public static String fromNamespaceEntry(String namespace) {
     if(namespace.charAt(0) != NAMESPACE_PREFIX)
       throw new IllegalArgumentException("Argument is not a valid namespace entry");
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
     Iterator<Tag> tagsIterator = CellUtil.tagsIterator(cell.getTagsArray(), cell.getTagsOffset(),
        cell.getTagsLength());
     while (tagsIterator.hasNext()) {
       Tag tag = tagsIterator.next();
       if (tag.getType() == ACL_TAG_TYPE) {
         // Deserialize the table permissions from the KV
         ListMultimap<String,Permission> kvPerms = ProtobufUtil.toUsersAndPermissions(
           AccessControlProtos.UsersAndPermissions.newBuilder().mergeFrom(
             tag.getBuffer(), tag.getTagOffset(), tag.getTagLength()).build());
         // Are there permissions for this user?
         List<Permission> userPerms = kvPerms.get(user.getShortName());
         if (userPerms != null) {
           results.addAll(userPerms);
         }
         // Are there permissions for any of the groups this user belongs to?
         String groupNames[] = user.getGroupNames();
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
