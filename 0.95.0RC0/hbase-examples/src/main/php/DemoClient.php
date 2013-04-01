<?php
/**
 * Copyright 2008 The Apache Software Foundation
 * 
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

# See {$THRIFT_HOME}/lib/php/README for additional help.

# Change this to match your thrift root.
$GLOBALS['THRIFT_ROOT'] = '/Users/irubin/Thrift/thrift-20080411p1/lib/php/src';

require_once( $GLOBALS['THRIFT_ROOT'].'/Thrift.php' );

# Something is wrong with this. Is this the PHP way of doing things? 
# Old versions of thrift seemingly worked with just a couple includes.
require_once( $GLOBALS['THRIFT_ROOT'].'/type/TMessageType.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/type/TType.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/exception/TException.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/factory/TStringFuncFactory.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/stringfunc/TStringFunc.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/stringfunc/Core.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php' );

# According to the thrift documentation, compiled PHP thrift libraries should
# reside under the THRIFT_ROOT/packages directory. Copy them there from gen-php/.
if (mkdir($GLOBALS['THRIFT_ROOT'].'/packages/Hbase', 0770, true)) {
  $files = scandir('gen-php/Hbase');
  foreach ($files as $file) {
    if (preg_match("/.*php$/", $file)) {
      copy("./gen-php/Hbase/$file", $GLOBALS['THRIFT_ROOT']."/packages/Hbase/$file"); 
    }
  }
}

require_once( $GLOBALS['THRIFT_ROOT'].'/packages/Hbase/Hbase.php' );
require_once( $GLOBALS['THRIFT_ROOT'].'/packages/Hbase/Types.php' );

use Thrift\Transport\TSocket;
use Thrift\Transport\TBufferedTransport;
use Thrift\Protocol\TBinaryProtocol;
use Hbase\HbaseClient;
use Hbase\ColumnDescriptor;
use Hbase\Mutation;
 
 
function printRow( $rowresult ) {
  echo( "row: {$rowresult->row}, cols: \n" );
  $values = $rowresult->columns;
  asort( $values );
  foreach ( $values as $k=>$v ) {
    echo( "  {$k} => {$v->value}\n" );
  }
}

$socket = new TSocket( 'localhost', 9090 );
$socket->setSendTimeout( 10000 ); // Ten seconds (too long for production, but this is just a demo ;)
$socket->setRecvTimeout( 20000 ); // Twenty seconds
$transport = new TBufferedTransport( $socket );
$protocol = new TBinaryProtocol( $transport );
$client = new HbaseClient( $protocol );

$transport->open();

$t = 'demo_table';

?><html>
<head>
<title>DemoClient</title>
</head>
<body>
<pre>
<?php

#
# Scan all tables, look for the demo table and delete it.
#
echo( "scanning tables...\n" );
$tables = $client->getTableNames();
sort( $tables );
foreach ( $tables as $name ) {
  echo( "  found: {$name}\n" );
  if ( $name == $t ) {
    if ($client->isTableEnabled( $name )) {
      echo( "    disabling table: {$name}\n");
      $client->disableTable( $name );
    }
    echo( "    deleting table: {$name}\n" );
    $client->deleteTable( $name );
  }
}

#
# Create the demo table with two column families, entry: and unused:
#
$columns = array(
  new ColumnDescriptor( array(
    'name' => 'entry:',
    'maxVersions' => 10
  ) ),
  new ColumnDescriptor( array(
    'name' => 'unused:'
  ) )
);

echo( "creating table: {$t}\n" );
try {
  $client->createTable( $t, $columns );
} catch ( AlreadyExists $ae ) {
  echo( "WARN: {$ae->message}\n" );
}

echo( "column families in {$t}:\n" );
$descriptors = $client->getColumnDescriptors( $t );
asort( $descriptors );
foreach ( $descriptors as $col ) {
  echo( "  column: {$col->name}, maxVer: {$col->maxVersions}\n" );
}
$dummy_attributes = array();
#
# Test UTF-8 handling
#
$invalid = "foo-\xfc\xa1\xa1\xa1\xa1\xa1";
$valid = "foo-\xE7\x94\x9F\xE3\x83\x93\xE3\x83\xBC\xE3\x83\xAB";

# non-utf8 is fine for data
$mutations = array(
  new Mutation( array(
    'column' => 'entry:foo',
    'value' => $invalid
  ) ),
);
$client->mutateRow( $t, "foo", $mutations, $dummy_attributes );

# try empty strings
$mutations = array(
  new Mutation( array(
    'column' => 'entry:',
    'value' => ""
  ) ),
);
$client->mutateRow( $t, "", $mutations, $dummy_attributes );

# this row name is valid utf8
$mutations = array(
  new Mutation( array(
    'column' => 'entry:foo',
    'value' => $valid
  ) ),
);
$client->mutateRow( $t, $valid, $mutations, $dummy_attributes );

# non-utf8 is not allowed in row names
try {
  $mutations = array(
    new Mutation( array(
      'column' => 'entry:foo',
      'value' => $invalid
    ) ),
  );
  $client->mutateRow( $t, $invalid, $mutations, $dummy_attributes );
  throw new Exception( "shouldn't get here!" );
} catch ( IOError $e ) {
  echo( "expected error: {$e->message}\n" );
}

# Run a scanner on the rows we just created
echo( "Starting scanner...\n" );
$scanner = $client->scannerOpen( $t, "", array( "entry:" ), $dummy_attributes );
try {
  while (true) printRow( $client->scannerGet( $scanner ) );
} catch ( NotFound $nf ) {
  $client->scannerClose( $scanner );
  echo( "Scanner finished\n" );
}

#
# Run some operations on a bunch of rows.
#
for ($e=100; $e>=0; $e--) {

  # format row keys as "00000" to "00100"
  $row = str_pad( $e, 5, '0', STR_PAD_LEFT );

  $mutations = array(
    new Mutation( array(
      'column' => 'unused:',
      'value' => "DELETE_ME"
    ) ),
  );
  $client->mutateRow( $t, $row, $mutations, $dummy_attributes );
  printRow( $client->getRow( $t, $row, $dummy_attributes ));
  $client->deleteAllRow( $t, $row, $dummy_attributes );

  $mutations = array(
    new Mutation( array(
      'column' => 'entry:num',
      'value' => "0"
    ) ),
    new Mutation( array(
      'column' => 'entry:foo',
      'value' => "FOO"
    ) ),
  );
  $client->mutateRow( $t, $row, $mutations, $dummy_attributes );
  printRow( $client->getRow( $t, $row, $dummy_attributes ));

  $mutations = array(
    new Mutation( array(
      'column' => 'entry:foo',
      'isDelete' => 1
    ) ),
    new Mutation( array(
      'column' => 'entry:num',
      'value' => '-1'
    ) ),
  );
  $client->mutateRow( $t, $row, $mutations, $dummy_attributes );
  printRow( $client->getRow( $t, $row, $dummy_attributes ) );

  $mutations = array(
    new Mutation( array(
      'column' => "entry:num",
      'value' => $e
    ) ),
    new Mutation( array(
      'column' => "entry:sqr",
      'value' => $e * $e
    ) ),
  );
  $client->mutateRow( $t, $row, $mutations, $dummy_attributes );
  printRow( $client->getRow( $t, $row, $dummy_attributes ));
  
  $mutations = array(
    new Mutation( array(
      'column' => 'entry:num',
      'value' => '-999'
    ) ),
    new Mutation( array(
      'column' => 'entry:sqr',
      'isDelete' => 1
    ) ),
  );
  $client->mutateRowTs( $t, $row, $mutations, 1, $dummy_attributes ); # shouldn't override latest
  printRow( $client->getRow( $t, $row, $dummy_attributes ) );

  $versions = $client->getVer( $t, $row, "entry:num", 10, $dummy_attributes );
  echo( "row: {$row}, values: \n" );
  foreach ( $versions as $v ) echo( "  {$v->value};\n" );
  
  try {
    $client->get( $t, $row, "entry:foo", $dummy_attributes );
    throw new Exception ( "shouldn't get here! " );
  } catch ( NotFound $nf ) {
    # blank
  }

}

$columns = array();
foreach ( $client->getColumnDescriptors($t) as $col=>$desc ) {
  echo("column with name: {$desc->name}\n");
  $columns[] = $desc->name.":";
}

echo( "Starting scanner...\n" );
$scanner = $client->scannerOpenWithStop( $t, "00020", "00040", $columns, $dummy_attributes );
try {
  while (true) printRow( $client->scannerGet( $scanner ) );
} catch ( NotFound $nf ) {
  $client->scannerClose( $scanner );
  echo( "Scanner finished\n" );
}
  
$transport->close();

?>
</pre>
</body>
</html>

