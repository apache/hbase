include Java
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.HRegionInfo
import org.apache.hadoop.hbase.HTableDescriptor
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.util.Writables

# This script goes through META for a table and checks
#that the table descriptor across regions is consistent.

#get table descriptor from meta row
def getTD(result)
  hri = Writables.getHRegionInfo(result.getValue(HConstants::CATALOG_FAMILY, HConstants::REGIONINFO_QUALIFIER))
  td = hri.getTableDesc()
  return td
end


# Name of this script
NAME = "check_schema_consistency"

# Print usage for this script
def usage
  puts 'Usage: %s.rb TABLE' % NAME
  exit!
end

# Check arguments
if ARGV.size != 1
  usage
else
  tableName = ARGV[0]
end

LOG = LogFactory.getLog(NAME)

c = HBaseConfiguration.new()
c.set("fs.default.name", c.get(HConstants::HBASE_DIR))

table = HTable.new(c, Bytes.toBytes(tableName))
td = table.getTableDescriptor()

LOG.info("Scanning META")
metaTable = HTable.new(c, HConstants::META_TABLE_NAME)
tableNameMetaPrefix = tableName + HConstants::META_ROW_DELIMITER.chr
scan = Scan.new(Bytes.toBytes(tableNameMetaPrefix + HConstants::META_ROW_DELIMITER.chr))
scanner = metaTable.getScanner(scan)
while (result = scanner.next())
  rowid = Bytes.toString(result.getRow())
  rowidStr = java.lang.String.new(rowid)
  if not rowidStr.startsWith(tableNameMetaPrefix)
    # Gone too far, break
    break
  end
  rtd = getTD(result)
  if !td.equals(rtd)
    LOG.warn("Meta table descriptor not consistent: " + rowid)
    LOG.info("Original Table desc: " + td.toString())
    LOG.warn("Region Table desc: " + rtd.toString())
  end
end
scanner.close()

LOG.info("Scan of META complete")
