#!/usr/bin/perl 

# szumak 2015 

my $version="1.0.0";
use Switch;
use POSIX;
use Getopt::Long;
use DBI;
use Config::Tiny;


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

### M A I N ###                                 << starting here
main();
### E  N  D ###

### Functions
#Main function where whole processing starts.
sub main 
{
    if ( $#ARGV == -1 ) { print_usage(); } #if
    GetOptions(
    			'c|config=s'      => \$CONF_PARAMS{'ConfigFilePath'},
                'help|?'          => sub{ print_usage() }
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
} #main
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
sub print_usage()
{
	print "$0 usage:\n";
	print "\t-c - configuration file for processing\n";
	print "\t-h - prints help\n";
	exit 0;
} #pritn_usage
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
#Writes a message to the log.
#       0 - info
#       1 - warning
#       2 - error
#       3 - verbose/debug mode
#       +10 - syslog - add 10 to the base to log into a syslog 
sub logM 
{
        my ($level,$message) = @_;
        if ( $level == 3 and ! defined $PARAMS{'Debug'} ) { return; }
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
