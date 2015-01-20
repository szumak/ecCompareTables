#!/usr/bin/perl 

# szumak 2015 

my $version="1.0.0";
use Switch;
use POSIX;
use Getopt::Long;
use DBI;
use Config::Tiny;
$| = 1;


### V A R S ###
#this variable contains global parameters which defines the script logic.
my %CONF_PARAMS;
#this variable contains the key word which are interpreted by the script. If you
#want to extend the configuration parameters list, you need to add the key word to the
#variable below. And after config.ini file will be readed you can read this parameter value
#from the hass array named "CONF_INI_PARAMS"
my @ConfigIniDict = ('port','ipaddr','username','password','dbname');
#this variable contains readed values from ini file. First 'general' block is readed, and later if same
#param is defined for the database the first is covered.
my %CONF_INI_PARAMS;
#Statistics. A structure containing information about the consistency
my %Statistics;

### M A I N ###                                 << starting here
$Statistics{'ScriptStart'} = time;
main();
### E  N  D ###

### Functions
#Main function where whole processing starts.
sub main 
{
    if ( $#ARGV == -1 ) { print_usage(); } #if
    GetOptions(
    			'c|config=s'          => \$CONF_PARAMS{'ConfigFilePath'},
    			'sdb|source_db=s'     => \$CONF_PARAMS{'SourceDB_Section'},
    			'ddb|dest_db=s'       => \$CONF_PARAMS{'DestDB_Section'},
    			'stbl|source_table=s' => \$CONF_PARAMS{'SourceTable'},
    			'dtbl|dest_table=s'   => \$CONF_PARAMS{'DestTable'},
    			'idf|id_field=s'	  => \$CONF_PARAMS{'IDField'},
    			'jp|justprint'	      => \$CONF_PARAMS{'JustPrintFLAG'},
    			'debug'			      => \$CONF_PARAMS{'Debug'},
                'help|?'              => sub{ print_usage() }
              );

 # -------------------- 
 # checking the parameters logic 
    if ( $CONF_PARAMS{'ConfigFilePath'} ) {
                if ( ! -e $CONF_PARAMS{'ConfigFilePath'} )
                {
                        logM(2,"--config parameter is defined but the file doesn't exists");
                        exit(0);
                } #if
	} else { 
		logM(2,"config file is not defined, use --config parameter");
	} #else
    
    if ( ! defined $CONF_PARAMS{'SourceDB_Section'} or ! defined $CONF_PARAMS{'DestDB_Section'} )
    {
    	logM(2,"Both parameters source_db and dest_db need to be defined");
    } #if 

    if ( ! defined $CONF_PARAMS{'SourceTable'} or ! defined $CONF_PARAMS{'DestTable'} )
    {
    	logM(2,"Both parameters source_table and dest_table need to be defined");
    } #if 

    if ( ! defined $CONF_PARAMS{'IDField'} )
    {
    	logM(2,"You need to specify ID field name for the table");
    } #if

 # ---------------------
 # reading the configuration file
 my $source_db_ini = readConfigFile( $CONF_PARAMS{'ConfigFilePath'}, $CONF_PARAMS{'SourceDB_Section'}, \@ConfigIniDict );
 my $dest_db_ini   = readConfigFile( $CONF_PARAMS{'ConfigFilePath'}, $CONF_PARAMS{'DestDB_Section'},   \@ConfigIniDict );

 # checking if u want only to print the configuration params 
 if ( defined $CONF_PARAMS{'JustPrintFLAG'} ) {
    printIniConfig( $source_db_ini,"source database",0); #last arg in the end says if you want to exit or continue;
    printIniConfig( $dest_db_ini,"destination database",1); #last arg in the end says if you want to exit or continue;
 } #if

 # ----------------------
 # connecting to database
	my $source_dbh = make_database_connection($source_db_ini);
	my $dest_dbh   = make_database_connection($dest_db_ini);
 # ----------------------
 # running main part
 program($source_dbh,$dest_dbh);
 print_data();

} #main
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub program()
{
	my ($source_dbh, $dest_dbh) = @_;
    $Statistics{'TotalConsistent'}   = 0;
    $Statistics{'TotalInconsistent'} = 0;
    $Statistics{'RowsOnBothSites'}   = 0;
    $Statistics{'RowsOnlyOnSource'}  = 0;
	my $sourceTableRowCount = getNumberOfrows($source_dbh,$CONF_PARAMS{'SourceTable'});
	my $destTableRowCount  = getNumberOfrows($dest_dbh,$CONF_PARAMS{'DestTable'});
	logM(3,"Source table row count: " . $sourceTableRowCount);
	logM(3,"Destination table row count: " . $destTableRowCount);
	if ( $sourceTableRowCount eq $destTableRowCount ) {
		logM(0,"Number of rows is equal: ".$destTableRowCount);
	} else {
		logM(1,"Different number of rows in tables: source(".$sourceTableRowCount.") destination(".$destTableRowCount.")");
	} #if
	# checking if both tables are empty 
	if ( $sourceTableRowCount eq 0 and $destTableRowCount eq 0 ) {
		logM(0,"Both tables are empty, nothing to check");
		exit(0);
	} #if
	# cheking if one of the tables is empty 
	if ( $destTableRowCount eq 0 ) {
		logM(2,"Destination table is empty");
	} #if

	if ( $sourceTableRowCount eq 0 ) {
		logM(2,"Source table is empty");
	} #if
	my $end = 0;
	my $i = 0; 
	while ( ! $end ) { 
		my ($SourceTbl_ptr,$IdsList_ptr, $idField_number) = getRows($source_dbh,100,$i);
        my $DestTbl_ptr;
		$i++;
		if ( $#{$SourceTbl_ptr} == -1 ) { 
			$end = 1; 
		} else {
				$DestTbl_ptr = getRowsFromArray($dest_dbh, $IdsList_ptr);
				compare_table_chunk($SourceTbl_ptr,$DestTbl_ptr,$IdsList_ptr, $idField_number);
		} #lese
	} #while
} #program
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub print_table_from_ptr
{
    my ($arrPtr) = @_;
    foreach ( @{$arrPtr} ) {
        foreach ( @{$_} ) {
            print $_ . ",";
        } #foreach
        print "\n";
    } #foreach
} #print_array
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub compare_table_chunk 
{
	my ($SourceTbl_ptr, $DestTbl_ptr, $IdsList_ptr, $idField_number) = @_;
	foreach ( @{$IdsList_ptr} ) {
		my $iter = 0;
        my $id = $_; #id which we are comparing 
		$srcIndex  = findArrIndex($SourceTbl_ptr, $id, $idField_number);
        $destIndex = findArrIndex($DestTbl_ptr,   $id, $idField_number);
        logM(3,"[compare_table_chunk] found indexes to compare: src(".$srcIndex.") dest(".$destIndex.")");
        if ( $srcIndex  < 0 ) {
            logM(2,"Id: ".$id." not found in the source database, this is weird");
        } # if
        if ( $destIndex < 0 ) {
            $Statistics{'RowsOnlyOnSource'}++;
            logM(1,"Id: ". $id ." not found in the destination database, :/"); 
        } else { 
            # I have two correct ids 
            $Statistics{'RowsOnBothSites'}++;
            compare_rows( ${$SourceTbl_ptr}[$srcIndex], ${$DestTbl_ptr}[$destIndex] );
        }   
	} # foreach 
} # compare_table_chunk
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub compare_rows 
{
    logM(3,"[compare_rows]");
    my ($rowA, $rowB) = @_;
    $Statistics{'TotalRows'}++;
    my $rowConsistent = 1; 
    for ( my $i=0; $i<=$#{$rowA}; $i++) {
        my $columnName = ${$Statistics{'OrderedColumnNames'}}[$i];
        if ( ${$rowA}[$i] eq ${$rowB}[$i] ) {
            $Statistics{'ColumnsHashConsistent'}->{$columnName}++;
        } else {
            $rowConsistent = 0;
            $Statistics{'ColumnsHashInconsistent'}->{$columnName}++;
        } # else
    } # for
    if ( $rowConsistent eq 1 ) {
            $Statistics{'TotalConsistent'}++;
        } else {
            $Statistics{'TotalInconsistent'}++;
        } #if
} # compare_rows
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub print_data
{
    $Statistics{'ScriptEnd'} = time;
    logM(0,"script ended in: " . ( $Statistics{'ScriptEnd'} - $Statistics{'ScriptStart'} ) . "[s]");
    logM(0,"Total rows checked: " . $Statistics{'TotalRows'} );
    logM(0,"Rows exists on both sites: " . $Statistics{'RowsOnBothSites'} );
    logM(0,"Rows exists only on source: " . $Statistics{'RowsOnlyOnSource'} );
    logM(0,"Total rows consistent: " . $Statistics{'TotalConsistent'} );
    logM(0,"Total rows inconsistent: " . $Statistics{'TotalInconsistent'} );
    foreach ( @{$Statistics{'OrderedColumnNames'}} ) {
        print $_ .": consistent(" . $Statistics{'ColumnsHashConsistent'}->{$_} . ") inconsistent(" . $Statistics{'ColumnsHashInconsistent'}->{$_} . ")\n";
    } #foreach 
} #print_data
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub findArrIndex 
{
    my ($SourceTbl_ptr,$id,$idField_number) = @_;
    my $index=0;
    foreach ( @{$SourceTbl_ptr} ){
        if ( ${$_}[$idField_number] eq  $id) {
            return $index; 
        } #if 
        $index++;
    } #foreach
    return -1; 
} #findArrIndex
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub getRowsFromArray # from destination
{
	my ($dbh,$IdsList_ptr) = @_;
	my @retArray=();
    my $ids = join(',',@{$IdsList_ptr}); 
	my $select_q = "select * from ".$CONF_PARAMS{'DestTable'}." where ". $CONF_PARAMS{'IDField'} . " in (".$ids.")";
	logM(3,"[getRowsFromArray] db rows selecting query: ".$select_q);
	logM(3,"[getRowsFromArray] ids count: " . ($#{$IdsList_ptr}+1));
	my $selSQL = $dbh->prepare( $select_q );
	my $queryStart = time();
    logM(3,"Executing query on the database");
    $selSQL->execute();
    my $queryEnd = time();
    logM(3,"Query executed in: ". ($queryEnd-$queryStart) . "[s]");
    while ( my $row = $selSQL->fetchrow_arrayref() ) {
   		my @array = @{$row};
    	push @retArray, \@array;
    } #while
    $selSQL->finish();
    return \@retArray;
} #getRowsFromArray
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub getRows #from source 
{
	my ($dbh,$row_c,$iter) = @_;
	my @retArray = ();
	my @idArray = (); # array which contains all the id's for search in destination database;
	my $select_q = "select * from ".$CONF_PARAMS{'SourceTable'}." order by ". $CONF_PARAMS{'IDField'}. " limit ".($row_c * $iter).",".$row_c;
	logM(3,"[getRows] db rows selecting query: ".$select_q);
	my $selSQL = $dbh->prepare($select_q);
	my $queryStart = time();
    logM(3,"Executing query on the database");
    $selSQL->execute();
    my $queryEnd = time();
    logM(3,"Query executed in: ". ($queryEnd-$queryStart) . "[s]");
    my $idField_number=0; #column number for ID field;
    # read about the bind_columns http://www.perlmonks.org/?node_id=7568
	# Here we are going to read column names and crete coresponding hash array
	my @columns;      # this array will contain references to the fields from hash array, this array will be passed later to the bind function
	my @column_names; # we need an order list of column names 
	my %ColumnsHash;  # this hash will keep filed values for one feached row, keys are the column names,
				      # you can get them sorted from column_names array
	my $c=0;
	foreach ( @{ $selSQL->{NAME_lc} } )  {
	        my $field = $_;
	        if ( $field eq lc($CONF_PARAMS{'IDField'} ) ) { $idField_number=$c }
	        #adding column names to the array, needed to keep order
	        push @column_names, $field;
	        $c++;
   	} #foreach
    #we are going to execute this block once 
    if ( ! defined $Statistics{'OrderedColumnNames'} ) {
        logM(3,"[getRows] Setting Statistics->ColumnsHash");
        my %a_hash;
        my %b_hash;
        my @arr; 
        foreach ( @{ $selSQL->{NAME_lc} } ) 
        {
            $a_hash{$_} = 0;
            $b_hash{$_} = 0;
            push @arr,$_;
        } # foreach
        $Statistics{'ColumnsHashConsistent'  } = \%a_hash;
        $Statistics{'ColumnsHashInconsistent'} = \%b_hash;
        $Statistics{'OrderedColumnNames'     } = \@arr;
    } #if
    while ( my $row = $selSQL->fetchrow_arrayref() ) {
    	my @array = @{$row};
    	push @retArray, \@array;
    	push @idArray, ${$row}[$idField_number];
    } #while
    $selSQL->finish();
    return (\@retArray,\@idArray, $idField_number);
} #getRows
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub print_usage()
{
	print "$0 usage:\n";
	print "\t-c --config - configuration file for processing\n";
	print "\t-sdb --source_db - section name for sourece db\n";
	print "\t-ddb --dest_db - section name for sourece db\n";
	print "\t-stbl --source_dbh - source database table name\n";
	print "\t-dtbl --dest_dbh - destination database table name\n";
	print "\t-idf --id_field - name of the table ID field\n";
	print "\t-jp  --just print the config ini parameters\n";
	print "\t-h - prints help\n";
	exit 0;
} #pritn_usage
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#Writes a message to the log.
#       0 - info
#       1 - warning
#       2 - error
#       3 - verbose/debug mode
sub logM 
{
        my ($level,$message) = @_;
        if ( $level == 3 and ! defined $CONF_PARAMS{'Debug'} ) { return; }
        my $prefix;
        switch( $level ) {
                case 0 { $prefix = "[Info]"; }
                case 1 { $prefix = "[Warning]"; }
                case 2 { $prefix = "[Error]"; }
                case 3 { $prefix = "[Debug]"; }
        } #switch
        $message = $prefix . " " . $message; 
       	print $0 . $message . "\n";
        switch( $level ) {
                case 2 { exit(2) }
        } #switch
} #log
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# make_database_connection
sub make_database_connection 
{
	my ($db_ini) = @_;
	my $connectionString = 'dbi:mysql:'. $db_ini->{'dbname'} .':'.$db_ini->{'ipaddr'}.':'.$db_ini->{'port'};
	logM(3,"[make_database_connection] dsn: ".$connectionString);
    my $dbh = DBI->connect( $connectionString,
        					$db_ini->{'username'}, $db_ini->{'password'}, 
        					{ PrintError => 1, AutoCommit => 1, RaiseError => 1 } )
              or logM(2,"database connection error");
    logM(3,"[make_database_connection] connected to database");
    return $dbh;
} # make_database_connection
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub trim($)
{
        my $string = shift;
        $string =~ s/^\s+//;
        $string =~ s/\s+$//;
        return $string;
} #trim
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
## This function just prints the configuration hash
sub printIniConfig 
{
    my ($hashPointer,$hashName,$exit) = @_;
    #printing master config
    print "======= $hashName ========\n";
    while ( my ($k,$v) = each (%$hashPointer) ) {
        if ( ! defined $v ) { $v = ""; } #don't want complaining about uninitialized value
        print "\t$k = $v \n";
    } #while

    if ( $exit != 0 ) {
        exit($exit);
    } #if
} #printIniConfig
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
##Reads config ini file. Exits if file doesn't exists. Function retuns
##the hash with the database configuration block
sub readConfigFile 
{
    my $ConfigIniFile = $_[0];
    my $DatabaseID    = $_[1];
    my @ConfigIniDict = @{$_[2]};
    my %DB;

    # print "file: $ConfigIniFile\n";
    # print "sectionId: $DatabaseID\n";
    # foreach ( @ConfigIniDict ) {
    #     print "ConfigIniDict : ". $_ ."\n";
    # }

    $DB{'section_name'} = $DatabaseID;
    #Check if file exists
    if ( ! -e $ConfigIniFile ) {
        logM(2,"Config file doesn't exists");
        exit(2);
    } #if
    if ( ! -r $ConfigIniFile ) {
        logM(2,"Cannot read config file. Permission denied");
        exit(2);
    } #if
    my $Config = Config::Tiny->read( $ConfigIniFile );

    # -- loading generic block to the %CONF_INI_PARAMS array
    foreach ( @ConfigIniDict ) {
        my $pname = $_;
        $DB{$pname} = $Config->{generic}->{$pname};
    } #foreach
    # -- checking if section in configuration file exists
    my $section = $Config->{$DatabaseID};
    if ( ! defined $section ) {
        logM(2,"Provided section doesn't exists in configuration file");
    } #ifvim
    # -- reloading database block to the %CONF_INI_PARAMS array
    foreach ( @ConfigIniDict ) {
        my $pname = $_;
        if ( defined $Config->{$DatabaseID}->{$pname} ) {
          $DB{$pname} = $Config->{$DatabaseID}->{$pname};
        } #if
    } #foreach
   return \%DB;
} #readConfigFile
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub getNumberOfrows 
{
        my ($dbh, $tableName) = @_;
        logM(3,"enter: getNumberOfrows tableName=".$tableName);
        if ( $tableName eq "" ) 
        {
        	logM(2,"getNumberOfrows: table name is empty");        	
        }
        my $retVal;
        my $query    = "select count(*) from $tableName;";
        my $sqlQuery  = $dbh->prepare($query)
            or die  "SQL prepare error. Can't prepare \'$query\': $dbh->errstr\n";
        my $rv = $sqlQuery->execute
            or die  "SQL prepare error. Can't execute the query \'$query\': $sqlQuery->errstr";
        while (@row= $sqlQuery->fetchrow_array()) {
                $retVal =  $row[0];
        } #while
        my $rc = $sqlQuery->finish;
        return $retVal;
} #getNumberOfrows
