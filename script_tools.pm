#!/usr/bin/perl -w

package script_tools;

use strict;
use File::Path qw(make_path);
use File::Copy;

# Open a script and write the common text to the file.

sub createScript {
  my $stdout = $_[0];
  my $text   = $_[1];
  my $proc   = $_[2];
  my $queue  = $_[3];
  my $script;

  if (defined $main::local) {$main::nodeDir = "$main::outputDirectory/working/$stdout";}
  if (!defined $main::nodeDir) {$main::nodeDir = "/scratch/$main::userID/$stdout";}
  if (defined $main::jobID) {$main::nodeDir = "$main::nodeDir\.$main::jobID";}

  open($script,">$stdout.sh");
  print $script ("# $text script created at $main::time\n\n");
  print $script ("#!/bin/bash\n");
  if (defined $main::nodeName) {
    print $script ("#PBS -l nodes=$main::nodeName+1:ppn=$proc");
  } else {
    print $script ("#PBS -l nodes=1:ppn=$proc");
  }
  if (defined $main::wallTime) {print $script (",walltime=$main::wallTime");}
  print $script ("\n");
  if ($queue ne "") {print $script ("#PBS -q $queue\n");}
  print $script ("\n");
  print $script ("echo `hostname`\n");
  print $script ("echo \$PBS_NODEFILE\n\n");
  print $script ("NODE_DIR=$main::nodeDir\n");
  print $script ("TEMP_DIR=\$NODE_DIR/tmp\n");
  print $script ("if [ ! -d \$TEMP_DIR ]; then mkdir -p \$TEMP_DIR; fi\n\n");
  if ($main::aligner == "mosaik" && $text =~ /align/i){
    print $script ("# Set the location of the tmp directory.\n\n");
    if (defined $main::tempDirectory) {
      print $script ("export MOSAIK_TMP = $main::tempDirectory\n");
      print $script ("if [ ! -d \$MOSAIK_TMP ]; then mkdir -p \$MOSAIK_TMP; fi\n\n");
    } else {
      print $script ("export MOSAIK_TMP=\$TEMP_DIR\n\n");
    }
  }
  print $script ("cd \$NODE_DIR\n\n");

  return $script;
}

# Define a function that deals with error handling for the software tools.
sub scriptFail {
  my $script = $_[0];
  my $stdout = $_[1];

  print $script ("#Define a function to terminate the script if a tool fails\n\n");
  print $script ("function Terminate_Script() {\n");
  print $script ("  FAILED_TOOL=\$1\n");
  print $script ("  FAIL_FILE=\"$stdout.fail\"\n");
  print $script ("  OUT=\$2\n");
  print $script ("  STDOUT=\$3\n");
  print $script ("  STDERR=\$4\n");
  print $script ("  FAILBIN=\"$main::outputDirectory/\$5\"\n");
  print $script ("  echo \"\$FAILED_TOOL failed.\" > \$FAIL_FILE\n");
  print $script ("  echo `hostname` >> \$FAIL_FILE\n");
  print $script ("  echo \$PBS_NODEFILE >> \$FAIL_FILE\n");
  print $script ("  if [ ! -d \$FAILBIN ]; then mkdir -p \$FAILBIN; fi\n\n");
  print $script ("  rsync \$NODE_DIR/\$FAIL_FILE \$FAILBIN\n");
  if ($main::copyOnFail eq "true") {print $script ("  if [ -f \$NODE_DIR/\$OUT ]; then rsync \$NODE_DIR/\$OUT \$FAILBIN; fi\n");}
  print $script ("  if [ -f \$NODE_DIR/\$STDOUT ]; then rsync \$NODE_DIR/\$STDOUT \$FAILBIN; fi\n");
  print $script ("  if [ -f \$NODE_DIR/\$STDERR ]; then rsync \$NODE_DIR/\$STDERR \$FAILBIN; fi\n");
  print $script ("  rm -fr \$NODE_DIR\n");
  print $script ("  exit\n");
  print $script ("}\n\n");
}

# Check for a failed tool and call Terminate_Script.

sub fail {
  my $script   = $_[0];
  my $tool     = $_[1];
  my $file     = $_[2];
  my $stdout   = $_[3];
  my $stderr   = $_[4];
  my $fail_dir = $_[5];

  print $script ("  if [ \$? -ne 0 ]; then \n");
  copyFiles($script, 0);
  print $script ("    Terminate_Script \"$tool\" $file $stdout $stderr $fail_dir\n");
  print $script ("  fi\n\n");
}

# Define a function that transfers files between the node and the local disk.
sub transferFiles {
  my $script = $_[0];;

  print $script ("# Define a function that transfers files between the node and local directory\n\n");
  print $script ("function TransferFiles() {\n");
  print $script ("  SOURCE=\$1\n");
  print $script ("  DESTINATION=\$2\n");
  print $script ("  INPUT_FILE=\$3\n");
  print $script ("  OUTPUT_FILE=\$4\n");
  print $script ("  if [ \"\$OUTPUT_FILE\" == \"\" ]; then OUTPUT_FILE=\$INPUT_FILE; fi\n");
  print $script ("  if [ ! -d \$DESTINATION ]; then mkdir -p \$DESTINATION; fi\n");
  print $script ("  rsync \$SOURCE/\$INPUT_FILE \$DESTINATION/\$OUTPUT_FILE\n");
  print $script ("}\n\n");
}

# Copy files back to the local disk.
sub copyFiles {
  my $script = $_[0];
  my $finish = $_[1];
  my $nokeys = scalar keys (%main::retainFiles);;
  my $text   = "";
  my %writtenDirectories;

  $text = ($finish == 0) ? "    " : "  ";
  if ($nokeys != 0) {
    if ($finish == 1) {
      print $script ("#\n# Copy requested files from the node back to the local disk, or\n");
      print $script ("# delete those not required from the local disk.\n#\n\n");
    }

# Ensure that the required directories exist.
    foreach my $key (keys %main::retainFiles) {
      if (!exists $writtenDirectories{$main::retainFiles{$key}}) {
        print $script ("    if [ ! -d $main::retainFiles{$key} ]; then mkdir -p $main::retainFiles{$key}; fi\n");
        $writtenDirectories{$main::retainFiles{$key}} = 1;
      }
    }
    print $script ("\n");

# Copy the files.
    foreach my $key (keys %main::retainFiles) {
      print $script ($text, "TransferFiles \$NODE_DIR $main::retainFiles{$key} $key\n");
    }


# Only delete files at the end of the script.
    foreach my $key (keys %main::deleteFiles) {
      if ($finish == 1) {print $script ("rm -f $main::deleteFiles{$key}/$key*\n");}
    }
    if ($finish == 1) {print $script ("\n");}
  }
}

# Write the final commands to the script and close.

sub finishScript {
  my $script     = $_[0];
  my $dir        = $_[1];
  my $scriptName = $_[2];
  my $scriptType = $_[3];

  if ($scriptType ne "") {$scriptType = "/$scriptType";}
  print $script ("# Delete the working directory from the node.\n\n");
  print $script ("  rm -fr \$NODE_DIR\n\n");
  if (defined $main::tempDirectory) {
    print $script ("# Remove the tmp directory\n\nrm -fr \$MOSAIK_TMP\n\n");
  }
  print $script ("#\n# Move the completed script to the CompletedScripts directory.\n#\n\n");
  print $script ("  SCRIPT=$scriptName.sh\n");
  print $script ("  SCRIPT_BIN=$dir/scripts$scriptType\n");
  print $script ("  COMPLETED_BIN=$dir/completedScripts$scriptType\n");
  print $script ("  if [ ! -d \$COMPLETED_BIN ]; then mkdir -p \$COMPLETED_BIN; fi\n\n");
  print $script ("  mv \$SCRIPT_BIN/\$SCRIPT \$COMPLETED_BIN\n");
  close($script);

# Check if data was added to the script.  If not, delete the script file.
# If so, move the script into the Scripts directory.
  my $path = "$dir/scripts$scriptType";
  if (! -d $path) {make_path($path);}
  move("$scriptName.sh","$path/$scriptName.sh");
}

1;
