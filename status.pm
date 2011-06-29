#!/usr/bin/perl -w

package status;

use strict;
use File::Find;

sub determineScriptStatus {

# Find all scripts in the $main::ResultsDir directory (and below).
  @main::foundScripts     = ();
  %main::runningOnCluster = onCluster("r");
  %main::queuedOnCluster  = onCluster("i");
  %main::existingSamples  = ();

  find(\&scriptSearch, $main::outputDirectory);
  find(\&failSearch, $main::outputDirectory);

  while($#main::foundScripts != -1) {
    my $scriptWithPath = pop(@main::foundScripts);
    my $script         = (split(/\//, $scriptWithPath))[-1];
    my $sample         = (split(/\./, $script))[0];

    if (! defined $main::existingSamples{$sample}) {$main::existingSamples{$sample}=1;}

# Search for completed align, merge and SNP calling scripts.
    if ($scriptWithPath =~ /completedScripts\/align/) {push(@{$main::completedAlignScripts{$sample}}, $scriptWithPath);}
    elsif ($scriptWithPath =~ /completedScripts\/merge/) {push(@{$main::completedMergeScripts{$sample}}, $scriptWithPath);}
    elsif ($scriptWithPath =~ /completedScripts/ && $script =~ /$main::snpCaller/) {push(@{$main::completedSnpScripts{$sample}}, $scriptWithPath);}

# Search for scripts in the queue.
    elsif (exists $main::queuedOnCluster{$script}) {
      if ($scriptWithPath =~ /scripts\/align/) {push(@{$main::queuedAlignScripts{$sample}}, $scriptWithPath);}
      elsif ($scriptWithPath =~ /scripts\/merge/) {push(@{$main::queuedMergeScripts{$sample}}, $scriptWithPath);}
      elsif ($scriptWithPath =~ /$main::snpCaller/) {push(@{$main::queuedSnpScripts{$sample}}, $scriptWithPath);}
    }

# search for scripts running on the cluster.
    elsif (exists $main::runningOnCluster{$script}) {
      if ($scriptWithPath =~ /scripts\/align/) {push(@{$main::runningAlignScripts{$sample}}, $scriptWithPath);}
      elsif ($scriptWithPath =~ /scripts\/merge/) {push(@{$main::runningMergeScripts{$sample}}, $scriptWithPath);}
      elsif ($scriptWithPath =~ /$main::snpCaller/) {push(@{$main::runningSnpScripts{$sample}}, $scriptWithPath);}
    }

# Search for failed jobs.
    elsif (exists $main::failedScripts{$script}) {
      if ($scriptWithPath =~ /scripts\/align/) {push(@{$main::failedAlignScripts{$sample}}, $scriptWithPath);}
      elsif ($scriptWithPath =~ /scripts\/merge/) {push(@{$main::failedMergeScripts{$sample}}, $scriptWithPath);}
      elsif ($scriptWithPath =~ /$main::snpCaller/) {push(@{$main::failedSnpScripts{$sample}}, $scriptWithPath);}
    }

# Search for align, merge and SNP calling scripts that have not yet been submitted.
    elsif ($scriptWithPath =~ /scripts\/align/) {push(@{$main::alignScripts{$sample}}, $scriptWithPath);}
    elsif ($scriptWithPath =~ /scripts\/merge/) {push(@{$main::mergeScripts{$sample}}, $scriptWithPath);}
    elsif ($script =~ /$main::snpCaller/) {push(@{$main::snpScripts{$sample}}, $scriptWithPath);}
  }
  if ($#main::foundScripts != -1) {
    print("Number of scripts remaining uncategorised:\t$#main::foundScripts+1\n");
  }

# Print to screen.
  printStatus();
}

# Print out script status information.
sub printStatus { 

# If the command line included -status only, only print out a limited
# amount of information.  If -status all, print out all information.
  if ($main::scriptStatus eq "") {$main::scriptStatus="simple";}
  if ($main::scriptStatus ne "simple" && $main::scriptStatus ne "all") {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("For pipeline status, specify either:\n");
    print("\t-status:\tprints a limited amount of information.\n");
    print("\t-status all: \tprints all status information\n");
    exit(1);
  }
  printScriptStatus();

# Count the number of completed, queued, running, unsubmitted and
# failes scripts.
  $main::totalNoScripts = 0;
  my %noCompletedAlign  = countScripts("Completed align scripts", \%main::completedAlignScripts);
  my %noCompletedMerge  = countScripts("Completed merge scripts", \%main::completedMergeScripts);
  my %noCompletedSnp    = countScripts("Completed SNP scripts", \%main::completedSnpScripts);
  my %noQueuedAlign     = countScripts("Queued align scripts", \%main::queuedAlignScripts);
  my %noQueuedMerge     = countScripts("Queued merge scripts", \%main::queuedMergeScripts);
  my %noQueuedSnp       = countScripts("Queued SNP scripts", \%main::queuedSnpScripts);
  my %noRunningAlign    = countScripts("Running align scripts", \%main::runningAlignScripts);
  my %noRunningMerge    = countScripts("Running merge scripts", \%main::runningMergeScripts);
  my %noRunningSnp      = countScripts("Running SNP scripts", \%main::runningSnpScripts);
  my %noFailedAlign     = countScripts("Failed align scripts", \%main::failedAlignScripts);
  my %noFailedMerge     = countScripts("Failed merge scripts", \%main::failedMergeScripts);
  my %noFailedSnp       = countScripts("Failed SNP scripts", \%main::failedSnpScripts);
  my %noAlign           = countScripts("Unsubmitted align scripts", \%main::alignScripts);
  my %noMerge           = countScripts("Unsubmitted merge scripts", \%main::mergeScripts);
  my %noSnp             = countScripts("Unsubmitted SNP scripts", \%main::snpScripts);
  print("\nTotal number of scripts:\t$main::totalNoScripts\n");

# Exit the pipeline script.  Only the status is provided when it is
# requested. No scripts are created.
  exit(0);
}

# Search for script files.
sub scriptSearch {
  chomp;
  if (/\.sh$/ && /$main::aligner/) {push(@main::foundScripts, "$File::Find::dir/$_");}
}

# Search for failed jobs.
sub failSearch {
  chomp;
  if (/\.fail$/ && /$main::aligner/) {
    my $temp=$_;
    $temp =~ s/\.fail$/\.sh/;
    $main::failedScripts{$temp}=1;
  }
}

# Look at the jobs queued or running on the cluster and check if any
# are from this aligner/SNP caller.
sub onCluster {
  my $queueOrRun      = $_[0];
  my %scriptOnCluster = ();

  my @clusterScripts = `qstat -$queueOrRun | grep $main::userID`;
  for (my $i = 0; $i < @clusterScripts; $i++) {
    my $script = $clusterScripts[$i];
    chomp($script);
    my $include = 0;
    my $nodeid  = 0;
    my $jobname = "";
    my $queue   = "";
    my $id=(split(/\./, $script))[0];
    my @scriptinfo    = `qstat -f $id`;
    my $rootDirectory = $main::outputDirectory;
    my @rootElements  = split(/\//, $rootDirectory);
    $rootDirectory    = $rootElements[0];
    for (my $i = 1; $i < scalar(@rootElements) - 2; $i++) {$rootDirectory = "$rootDirectory/$rootElements[$i]";}

    for (my $j = 0; $j < @scriptinfo; $j++) {
      my $line = $scriptinfo[$j];
      chomp($line);
      while (exists $scriptinfo[$j + 1] && $scriptinfo[$j + 1] =~ /^\t/) {
        my $temp = $scriptinfo[$j + 1];
        chomp($temp);
        $temp =~ s/\s//g;
        $line = join("", $line, $temp);
        $j++;
      }
      if ($line =~ /Job_Name/) {
        $jobname = $line;
        $jobname = (split(/=/, $jobname))[1];
        $jobname =~ s/\s//g;
      }
      if ($line =~ /queue/) {
        $queue = $line;
        $queue = (split(/\=/, $queue))[1];
        $queue =~ s/\s//g;
      }
      if (($line =~ /Output_Path/) && ($line =~ /$main::rootDirectory/)) {
        $include = 1;
      }
      if ($queueOrRun eq "r"){
        if ($line =~ /exec_host/) {
          $nodeid = $line;
          $nodeid = (split(/\=/, $nodeid))[1];
          $nodeid =~ /(node\d+)/;
          $nodeid = $1;
        }
      } else {
        $nodeid="0";
      }
    }
    $id = join("\t", $id, $queue, $nodeid);
    if ($include == 1) {$scriptOnCluster{$jobname} = $id;}
  }

  return(%scriptOnCluster);
}

# Print script status to screen.
sub printScriptStatus {
  foreach my $sample (sort keys %main::existingSamples) {
    if ($main::scriptStatus eq "all") {
      print("Scripts for sample:\t$sample\n");
      printScripts("Completed align scripts", $sample, \%main::completedAlignScripts);
      printScripts("Completed merge scripts", $sample, \%main::completedMergeScripts);
      printScripts("Completed SNP scripts", $sample, \%main::completedSnpScripts);
      printScripts("Queued align scripts", $sample, \%main::queuedAlignScripts);
      printScripts("Queued merge scripts", $sample, \%main::queuedMergeScripts);
      printScripts("Queued SNP scripts", $sample, \%main::queuedSnpScripts);
      printScripts("Running align scripts", $sample, \%main::runningAiignScripts);
      printScripts("Running merge scripts", $sample, \%main::runningMergeScripts);
      printScripts("Running SNP scripts", $sample, \%main::runningSnpScripts);
      printScripts("Unsubmitted align scripts", $sample, \%main::alignScripts);
      printScripts("Unsubmitted merge scripts", $sample, \%main::mergeScripts);
      printScripts("Unsubmitted SNP scripts", $sample, \%main::snpScripts);
      printScripts("Failed align scripts", $sample, \%main::failedAlignScripts);
      printScripts("Failed merge scripts", $sample, \%main::failedMergeScripts);
      printScripts("Failed SNP scripts", $sample, \%main::failedSnpScripts);
    }
    if (exists $main::failedAlignScripts{$sample} ||
        exists $main::failedMergeScripts{$sample} ||
        exists $main::failedSnpScripts{$sample}) {
      if ($main::scriptStatus eq "simple") {print("Failed scripts for sample:\t$sample\n");}
    }
  }
  print("\n");
}

# Print the scripts.
sub printScripts {
  my $text   = $_[0];
  my $sample = $_[1];
  my %hash   = %{$_[2]};

  if (exists $hash{$sample}) {
    print("\t$text:\n");
    my @array = @{$hash{$sample}};
    foreach my $script (@array) {print("\t\t$script\n");}
  }
}

# Count the number of scripts in each category (e.g. completed, unsubmitted etc.).
sub countScripts {
  my $text       = $_[0];
  my %inputHash  = %{$_[1]};
  my %outputHash = ();

  foreach my $sample (keys %inputHash) {
    foreach my $script (@{$inputHash{$sample}}) {
      $outputHash{$sample}++;
      $outputHash{"Total"}++;
    }
  }

  if (exists $outputHash{"Total"}) {
    print("$text:\t$outputHash{\"Total\"}\n");
    $main::totalNoScripts += $outputHash{"Total"};
  }

  return(%outputHash);
}

1;
