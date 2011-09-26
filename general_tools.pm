#!/usr/bin/perl -w

package general_tools;

use strict;
use File::Path;

# Find the current date and time.
sub findTime {
  my @months=qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);
  my @days=qw(Sun Mon Tue Wed Thu Fri Sat);
  my ($second,$minute,$hour,$dayofmonth,$month,$year,$dayofweek,$dayofyear,$daylightsavings);
  my $time;

  ($second,$minute,$hour,$dayofmonth,$month,$year,$dayofweek,$dayofyear,$daylightsavings)=localtime();
  $year+=1900;
  $time=sprintf("%02d%s%02d%s%02d%s",$hour,":",$minute,":",$second," on $days[$dayofweek], $months[$month] $dayofmonth,$year");
  $month+=1;

  return ($time,$dayofmonth,$month,$year);
}

# Initialise variables used in the pipeline.
sub initialise {
  $main::numberTotalMerge = 0;
  $main::numberComplete   = 0;
  $main::numberWithdrawn  = 0;
  $main::numberAdd        = 0;
  $main::numberRemoved    = 0;
  $main::numberCreated    = 0;

  # If no scratch disk is provided, set the default.
  if (!defined $main::scratch) {$main::scratch = "/scratch";}
}

# Check that the supplied file exists.
sub checkFileExists {
  my $file = $_[0];

  if (! -f $file) {
    print("\n***SCRIPT TERMINATED***\n");
    print("File: $file does not exist.\n");
    exit(1);
  }
}

# Generate the directory structure for all the created files.  Only
# perform this step if the directory does not already exist.
sub generateDirectory {

  # If a directory name has not been defined, default to 'Pipeline'.
  if (! defined $main::outputDirectory) {$main::outputDirectory = "pipeline";}

  # Create the working directory name.  This depends on whether SNP calling or
  # alignemnts are being performed.
  $main::outputDirectory = "$main::cwd/$main::outputDirectory";

  if ($main::aligner ne "none") {
    if (! -d "$main::outputDirectory/$main::aligner") {mkpath("$main::outputDirectory/$main::aligner");}
  } elsif ($main::snpCaller ne "none") {
    if (! -d "$main::outputDirectory/$main::snpCaller") {mkpath("$main::outputDirectory/$main::snpCaller");}
  }
}

# For a tab delimited string, where each entry is of the form tag:value, this
# routine takes the tag as an input and returns the value.

sub findTag {
  my ($string, $tag) = @_;
  my $value = "undefined";
  my @info;

  @info = split(/\t/, $string);
  for (my $i=0; $i<@info; $i++) {
    my $infoTag = (split(/:/, $info[$i]))[0];
    if ($infoTag =~ /^$tag$/) {$value = (split(/:/, $info[$i],2))[1];}
  }

  return $value;
}

# Set the input directory and input file.
sub setInputs {
  my ($script, $stdout, $input, $input2) = @_;
  my $location  = "";

  # If the output location isn't specified, assume it is the same as the
  # location of the input file.
  if (!defined $main::modules{$main::task->{TASK}}->{INPUT}) {$location = $main::task->{LOCATION};}

  # Check the input and output locations required by the task.  If the input location
  # is the node and the file is not located on the the node, copy it across.
  # First ensure that fthe files are required on the node, they are copied across.
  if ($main::modules{$main::task->{TASK}}->{INPUT} eq "local" || $location eq "local") {
    if ($main::task->{PATH} ne "") {print $script ("  INPUT_DIR=$main::task->{PATH}\n");}
    print $script ("  INPUT=$input\n");
    if ($input2 ne "") {print $script ("  INPUT2=$input2\n");}
  } elsif ($main::modules{$main::task->{TASK}}->{INPUT} eq "node" || $location eq "node") {
    print $script ("  INPUT_DIR=\$NODE_DIR\n");
    print $script ("  INPUT=$input\n");
    if ($input2 ne "") {print $script ("  INPUT2=$input2\n");}
    if ($main::task->{LOCATION} eq "local") {
      print $script ("  TransferFiles $main::task->{PATH} \$INPUT_DIR \$INPUT\n");
      if ($input2 ne "") {print $script ("  TransferFiles $main::task->{PATH} \$INPUT_DIR \$INPUT2\n");}
      $main::task->{LOCATION}="node";
    }
  } else {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("Unknown source directory: $main::modules{$main::task->{TASK}}->{INPUT}\n");
    die("Error in general_tools::setInputs.\n");
  }
  print $script ("\n");
}

# Set the output directory and output file.
sub setOutputs {
  my ($script, $stdout, $output) = @_;
  my $outputDir = "";
  my $location  = "";

  # If the output location isn't specified, assume it is the same as the
  # location of the input file.
  if (!defined $main::modules{$main::task->{TASK}}->{OUTPUT}) {$location = $main::task->{LOCATION};}

  if ($main::modules{$main::task->{TASK}}->{OUTPUT} eq "local" || $location eq "local") {
    if ($stdout ne "") {
      if ($main::aligner eq "none") {
        $outputDir = "$main::outputDirectory/$main::modules{$main::task->{TASK}}->{DIR}";
      } else {
        $outputDir = "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/$main::modules{$main::task->{TASK}}->{DIR}";
      }
    } else {
      if ($main::aligner eq "none") {
        $outputDir = "$main::outputDirectory/$main::modules{$main::task->{TASK}}->{DIR}";
      } else {
        $outputDir = "$main::outputDirectory/$main::aligner/$main::modules{$main::task->{TASK}}->{DIR}";
      }
    }
    print $script ("  OUTPUT_DIR=$outputDir\n");
    if ($output ne "") {print $script ("  OUTPUT=$output\n");}

    # Ensure that the output directory exists.
    print $script ("  if [ ! -d \$OUTPUT_DIR ]; then mkdir -p \$OUTPUT_DIR; fi\n");
  } elsif ($main::modules{$main::task->{TASK}}->{OUTPUT} eq "node" || $location eq "node") {
    print $script ("  OUTPUT_DIR=\$NODE_DIR\n");
    if ($output ne "") {print $script ("  OUTPUT=$output\n");}
  } else {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("Unknown destination directory: $main::modules{$main::task->{TASK}}->{OUTPUT}\n");
    die("Error in general_tools::setOutputs.\n");
  }
  print $script ("\n");
}

# If the input file to the task is not to be retained, remove the file to
# save space.
sub removeInput {
  my ($script, $stdout, $file) = @_;

# If the file is to be retained after completion, add the file to the
# RetainFiles hash.
  my $retain = $main::modules{$main::task->{TASK}}->{RETAIN};
  my $sample = $main::sampleInfo{$stdout}->{SAMPLE};
  my $dir    = $main::modules{$main::task->{TASK}}->{DIR};
  if ($retain eq "yes" && $main::task->{LOCATION} eq "node") {
    $main::retainFiles{$file} = "$main::outputDirectory/$main::aligner/$sample/$dir";
  }

# Remove input files that are no longer required.
  if ($main::task->{RETAIN_INPUT} eq "no") {
    print $script ("# Remove the input file to save space.\n\n");
    print $script ("  rm -f \$INPUT_DIR/$file\n\n");
  }
}

# Update the location and the file in main::Task.
sub updateTask {
  my ($stdout, $file) = @_;
  my $outputDir;
  my $location;

  # If the output location isn't specified, assume it is as specified in main::task->{LOCATION}.
  if (!defined $main::modules{$main::task->{TASK}}->{OUTPUT}) {$location = $main::task->{LOCATION};}
  
  if ($main::modules{$main::task->{TASK}}->{OUTPUT} eq "local" || $location eq "local") {
    if ($stdout ne "") {
      $outputDir = "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/$main::modules{$main::task->{TASK}}->{DIR}";
    } else {
      $outputDir = "$main::outputDirectory/$main::aligner/$main::modules{$main::task->{TASK}}->{DIR}";
    }
    $main::task->{LOCATION} = "local";
    $main::task->{PATH}     = $outputDir;
  } elsif ($main::modules{$main::task->{TASK}}->{OUTPUT} eq "node" || $location eq "node") {
    $main::task->{LOCATION} = "node";
    $main::task->{PATH}     = "$main::nodeDir";
  }
  $main::task->{FILE} = $file;
}

# At the end of each task, change the task contained in main::Task to the next task in the
# pipeline.  This does not amend the files etc. contained in main::Task as not all tasks
# require this information to be amended.
sub iterateTask {
  my $stdout      = $_[0];
  my @tasks       = @{$_[1]};
  my $numberTasks = scalar(@tasks) - 1;

# Update main::Task to reflect the next task in the pipeline.
  if ($main::task->{TASKID} == $numberTasks) {
    $main::task->{TASK} = "Complete";
  } else {
    $main::task->{RETAIN_INPUT} = $main::modules{$main::task->{TASK}}->{RETAIN};
    $main::task->{TASKID}++;
    $main::task->{TASK} = $tasks[$main::task->{TASKID}];
  }
}

1;
