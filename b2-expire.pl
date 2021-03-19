#!/usr/bin/perl
#
#  b2-expire.pl: Use this to query and expire files from Backblaze
#  Disclaimer: No guarantees this works as intended. Data loss is likely.
#  Do not use without understanding what this is for and how it works.
#

use strict;
use warnings;
use Mojo::UserAgent;
use DateTime;
use Sys::Syslog;
use File::Basename;
use MIME::Base64 qw( encode_base64 );

my $retentionDays = 30; # Set to desired value
my $appID = 'your appID';
my $appKey = 'your appKey';
my $accountID = 'your accountID';
my $bucketID = 'your bucketID';
my $creds = encode_base64($appID . ':' . $appKey, '');
my $authURL = 'https://api.backblazeb2.com/b2api/v2/';
my $authToken;
my $apiURL;
my $nextFile = undef;
my $expireDays = DateTime->now()->subtract(days => $retentionDays)->epoch();
my %oldFiles;
my %hiddenFiles;

####### public static void main #######
($authToken, $apiURL) = getAuthToken();
while (defined($nextFile = listFiles($nextFile))) {} #Have to loop as B2 paginates output to 10K items
expireFiles(); #Delete all the (old) things! 
exit 0;


####### Subroutines #######

sub getAuthToken {
    my $ua = Mojo::UserAgent->new;
    my $url = "$authURL" . "b2_authorize_account";
    my $tx = $ua->get($url => {Accept => 'application/json', Authorization => "Basic $creds"});
    
    if (my $err = $tx->error) {
        die "Connection to Backblaze API failed with HTTP $err->{code}";
    } else {
        my $json = $tx->res->json;
        $authToken = $json->{'authorizationToken'};
        $apiURL = $json->{'apiUrl'};
    } 
    return ("$authToken", "$apiURL");
}

sub listFiles {
    my $nextFile = shift;
    my $json = {bucketId => $bucketID, maxFileCount => 10000};
    $json->{startFileName} = $nextFile if defined($nextFile);

    my $ua = Mojo::UserAgent->new;
    my $url = "$apiURL" . '/b2api/v2/b2_list_file_versions';
    my $tx = $ua->post($url => {Accept => 'application/json', Authorization => $authToken} => json => $json);

    if (my $err = $tx->error) {
        die "Listing files failed with HTTP $err->{code}";
    } else {
        my $payload = $tx->res->json;
        my @files = @{$payload->{'files'}};
        foreach my $file (@files) {
            if ($file->{'action'} eq "hide") { #Enumerate hidden files in case we're calling unhide()
                $hiddenFiles{$file->{'fileName'}} = $file->{'fileId'};
            }
            my $fileName = $file->{'fileName'};
            my $fileDate = $file->{'uploadTimestamp'};
            my $fileId = $file->{'fileId'};
            $fileDate = ($fileDate / 1000);
            if ($fileDate < $expireDays) {
                push @{$oldFiles{$file->{'fileName'}}}, $file->{fileId};
            }
        }
    }
    
    if ($tx->res->json->{nextFileName}) {
        return $tx->res->json->{nextFileName};
    } else {
        return undef; # No more files
    }
}

sub expireFiles {
    foreach my $filename (keys %oldFiles) {
        my @fileids = @{$oldFiles{$filename}};
        foreach my $fileid(@fileids) {
            my $deleteJSON = {fileName => $filename, fileId => $fileid};
            my $ua = Mojo::UserAgent->new;
	    $ua = $ua->inactivity_timeout(60);
            my $url = "$apiURL" . '/b2api/v2/b2_delete_file_version';
            my $tx = $ua->post($url => {Accept => 'application/json', Authorization => $authToken} => json => $deleteJSON);
            if (my $err = $tx->error) {
		if ($err->{code}) {
                    logger("Expiring file $filename failed with HTTP error code $err->{code}");
                    if ($err->{message}) {
                        logger("Message expiration failed with $err->{message}"); #We should simply note this and continue.
                    }
		} else {
                    logger("Expiring file $filename failed: $err->{message}");
                }
            }
        }
    }
}

sub logger {
    my $message = shift;
    openlog(basename($0), 'ndelay,pid', 'user');
    syslog("warning", $message);
    closelog();
}

###############################################################################
#   Unused Subroutines:                                                       #
#   These may come in handy for debugging, cleaning up, etc. but              #
#   aren't currently in use by the code.                                      #
###############################################################################

sub hideFile {
    my $hideFile = shift;
    my $json = {bucketId => $bucketID, fileName => $hideFile};
    my $ua = Mojo::UserAgent->new;
    my $url = "$apiURL" . '/b2api/v2/b2_hide_file';
    my $tx = $ua->post($url => {Accept => 'application/json', Authorization => $authToken} => json => $json);
    if (my $err = $tx->error) {
        logger("Hiding file $hideFile failed with HTTP $err->{code}"); 
    } 
}

sub unhideFiles { 
    foreach my $key (keys %hiddenFiles) {
        my $filename = $key;
        my $fileid = $hiddenFiles{$key};
        my $deleteJSON = {fileName => $filename, fileId => $fileid};
        my $ua = Mojo::UserAgent->new;
        my $url = "$apiURL" . '/b2api/v2/b2_delete_file_version';
        my $tx = $ua->post($url => {Accept => 'application/json', Authorization => $authToken} => json => $deleteJSON);
        if (my $err = $tx->error) {
            print "Unhiding file $filename failed: $err->{code}\n";
        } else {
            print "Unhid file $filename with ID $fileid\n";
        }
    }
}

sub listBuckets {
    #Note the requirement for accountId.
    my $ua = Mojo::UserAgent->new;
    my $url = "$apiURL" . '/b2api/v2/b2_list_buckets';
    my $tx = $ua->post($url => {Accept => 'application/json', Authorization => $authToken} => json => {accountId => $accountID});
    if (my $err = $tx->error) {
        die "Connection to Backblaze API failed with HTTP $err->{code}";
    } else {
        my $payload = $tx->res->json;
        #do whatever you want with this
    }
}
