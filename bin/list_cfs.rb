# List all tables in a human and machine readable format.
# table[tab]cf1,cf2,cf3
#
#  ${HBASE_HOME}/bin/hbase org.jruby.Main list_cfs.rb

require 'java'

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.HConstants
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.util.Bytes

config = HBaseConfiguration.new
config.set 'fs.default.name', config.get(HConstants::HBASE_DIR)

admin = HBaseAdmin.new(config)

scanner = admin.listTables
tables = {}

scanner.each { |row|
  table = row.getNameAsString
  cfs = []
  a = row.getFamilies.each { |cf_java|
    cf = Bytes.toString(cf_java.getName)
    cfs[0,0] = cf
  }
  tables[table] = cfs.uniq
}

tables.keys.each { |table|
  cfs = tables[table].sort.join(',')
  puts "%s\t%s" % [table, cfs]
}
