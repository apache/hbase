package org.apache.hadoop.hbase;

import edu.umd.cs.findbugs.annotations.Nullable;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.yetus.audience.InterfaceAudience;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * This class has been replaced by CatalogAccessor.
 * It is currently kept as a proxy for CatalogAccessor to ease
 * the review of the HBASE-11288 (split meta) patch. This class will
 * be remove as soon as this patch is committed. Which should be
 * pretty much a simple search and replace operation.
 */
@InterfaceAudience.Private
public class MetaTableAccessor extends CatalogAccessor {

  /**
   * Put the passed <code>ps</code> to the <code>hbase:meta</code> table.
   * @param connection connection we're using
   * @param ps Put to add to hbase:meta
   */
  public static void putsToMetaTable(final Connection connection, final List<Put> ps)
      throws IOException {
    putsToCatalogTable(connection, TableName.META_TABLE_NAME, ps);
  }

  /**
   * Callers should call close on the returned {@link Table} instance.
   * @param connection connection we're using to access Meta
   * @return An {@link Table} for <code>hbase:meta</code>
   * @throws NullPointerException if {@code connection} is {@code null}
   */
  public static Table getMetaHTable(final Connection connection)
    throws IOException {
    // We used to pass whole CatalogTracker in here, now we just pass in Connection
    Objects.requireNonNull(connection, "Connection cannot be null");
    if (connection.isClosed()) {
      throw new IOException("connection is closed");
    }
    return connection.getTable(TableName.META_TABLE_NAME);
  }

  public static void scanMeta(Connection connection,
    @Nullable final byte[] startRow, @Nullable final byte[] stopRow,
    ClientMetaTableAccessor.QueryType type, int maxRows,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    scanCatalog(connection,
      TableName.META_TABLE_NAME,
      startRow,
      stopRow,
      type,
      null,
      maxRows,
      visitor);
  }

  public static void scanMeta(Connection connection,
    @Nullable final byte[] startRow, @Nullable final byte[] stopRow,
    ClientMetaTableAccessor.QueryType type, @Nullable Filter filter, int maxRows,
    final ClientMetaTableAccessor.Visitor visitor) throws IOException {
    scanCatalog(connection,
      TableName.META_TABLE_NAME,
      startRow,
      stopRow,
      type,
      filter,
      maxRows,
      visitor);
  }
}
