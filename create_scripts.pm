#!/usr/bin/perl -w

package create_scripts;

use strict;

# Check to see if any jobs are listed as failed and include an
# additional status tag to reflect this.
sub checkFailed {
  foreach my $stdout (keys %main::sampleInfo) {
    foreach my $check (@{$main::failedAlignScripts{$main::sampleInfo{$stdout}->{SAMPLE}}}) {
      my @fileTags = split(/\./, $check);
      my $run      = $fileTags[1];
      my $readType = $fileTags[-3];
      if ($readType eq "SINGLE") {$main::runInfo{$run}->{SE_STATUS} = "failed";}
      if ($readType eq "PAIRED") {$main::runInfo{$run}->{PE_STATUS} = "failed";}
    }
  }
}

# Start working through each sample x technology pair and generate all
# necessary scripts.

sub createScripts {
  my $stdoutCount = 1;
  my $bam;

  if (scalar keys %main::sampleInfo >= 40) {print("0%------------------50%-----------------100%\n");}
  foreach my $stdout (sort keys %main::sampleInfo) {

# If alignment scripts are required, generate them here,
    if ($main::sampleInfo{$stdout}->{ALIGN} eq "yes") {
      foreach my $run (keys %{$main::sampleInfo{$stdout}->{RUN}}) {
        if (exists $main::runInfo{$run}->{FASTQ}) {
          if ($main::runInfo{$run}->{STATUS} eq "align" || $main::runInfo{$run}->{STATUS} eq "realign") {
            $main::task = {};
            initialiseTask($stdout, $run, "SINGLE");
            if ($main::task->{TASK} ne "Complete") {
              $bam = createAlignmentScript($stdout, $run, "SINGLE");
            } else {
              $bam = "$main::task->{PATH}/$main::task->{FILE}";
            }
            push(@{$main::sampleInfo{$stdout}->{ALIGNED_BAM}}, $bam);
          }
        }

        if (exists $main::runInfo{$run}->{FASTQ1}) {
          if ($main::runInfo{$run}->{STATUS} eq "align" || $main::runInfo{$run}->{STATUS} eq "realign") {
            $main::task = {};
            initialiseTask($stdout, $run, "PAIRED");
            if ($main::task->{TASK} ne "Complete") {
              $bam = createAlignmentScript($stdout, $run, "PAIRED");
            } else {
              $bam = "$main::task->{PATH}/$main::task->{FILE}";
            }
            push(@{$main::sampleInfo{$stdout}->{ALIGNED_BAM}}, $bam);
          }
        }
      }
    }

# For merged bam files marked as complete, check if base quality recalibration
# and/or duplicate marking need to be performed.
    $main::mergeFileName = $stdout;
    $main::numberTotalMerge++;
    my $file = "$main::ouptutDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/merged/$main::mergeFileName.bam";
    if ($main::sampleInfo{$stdout}->{STATUS} eq "complete" && ! -e $file) {
      transferMergedBam($stdout);
    }

# Create the merge script.
    if ($main::sampleInfo{$stdout}->{STATUS} ne "complete" && $main::sampleInfo{$stdout}->{HASFASTQ} eq "true") {
      createMergeScript($stdout);
      if ($main::sampleInfo{$stdout}->{STATUS} ne "add") {$main::numberCreated++;}

# Generate glf files.
    #} elsif ($main::snpCaller =~ /^glf/) {
    #  glfSuite::pileup($stdout);
    }

    if (scalar keys %main::sampleInfo >= 40) {
      if ($stdoutCount == 1) {print(".");}
      $stdoutCount++;
      if ($stdoutCount > $main::numberStdout/40) {$stdoutCount = 1;}
    }
  }
  if (scalar keys %main::sampleInfo >= 40) {print("\n\n");}
}

# Set tasks to the beginning of the alignment pipeline.
sub initialiseTask {
  my $stdout   = $_[0];
  my $run      = $_[1];
  my $readType = $_[2];
  my $sample;
  my $technology;

  # Check to see that the fastq file exists.
  if ($readType eq "SINGLE") {
    if (exists $main::fastq{$main::runInfo{$run}->{FASTQ}}) {
      $main::task = {
        TASK     => $main::alignTasks[0],
        TASKID   => 0,
        LOCATION => "local",
        READTYPE => "SINGLE",
        PATH     => $main::fastq{$main::runInfo{$run}->{FASTQ}},
        FILE     => $main::runInfo{$run}->{FASTQ},
      };
      $main::sampleInfo{$stdout}->{HASFASTQ} = "true";
    } else {
      $sample     = $main::sampleInfo{$stdout}->{SAMPLE};
      $technology = $main::sampleInfo{$stdout}->{TECHNOLOGY};
      push(@{$main::missingFastq{"$sample.$technology"}}, "$run (single-end)");
      $main::task->{TASK}="Complete";
    }
  } elsif ($readType eq "PAIRED") {
    if (exists $main::fastq{$main::runInfo{$run}->{FASTQ1}} && exists $main::fastq{$main::runInfo{$run}->{FASTQ2}}) {
      $main::task = {
        TASK     => $main::alignTasks[0],
        TASKID   => 0,
        LOCATION => "local",
        READTYPE => "PAIRED",
        PATH     => $main::fastq{$main::runInfo{$run}->{FASTQ1}},
        FILE     => $main::runInfo{$run}->{FASTQ1},
        FILE2    => $main::runInfo{$run}->{FASTQ2},
      };
      $main::sampleInfo{$stdout}->{HASFASTQ} = "true";
    } else {
      $sample     = $main::sampleInfo{$stdout}->{SAMPLE};
      $technology = $main::sampleInfo{$stdout}->{TECHNOLOGY};
      push(@{$main::missingFastq{"$sample.$technology"}}, "$run (paired-end)");
      $main::task->{TASK}="Complete";
    }
  }
}

# Create the necessary alignment scripts.
sub createAlignmentScript {
  my $stdout   = $_[0];
  my $run      = $_[1];
  my $readType = $_[2];
  my $dir;
  my $bam;

  ($main::runFileName=$stdout) =~ (s/$main::runInfo{$run}->{SAMPLE}/$main::runInfo{$run}->{SAMPLE}.$run/);
  $main::runFileName =~ s/$main::sampleInfo{$stdout}->{DATE}/$readType.$main::sampleInfo{$stdout}->{DATE}/;
  %main::retainFiles = ();
  %main::deleteFiles = ();
  $main::renameFile  = "$main::runFileName.bam";
  $main::modules{RENAME_BAM}->{DIR} = "bam";

  $main::SCRIPT = script_tools::createScript($main::runFileName, "align", 8, "bigmem");
  script_tools::scriptFail($main::SCRIPT, $main::runFileName);
  script_tools::transferFiles($main::SCRIPT);
  while($main::task->{TASK} ne "Complete") {
    if ($main::modules{$main::task->{TASK}}->{DIR} ne "") {$dir = $main::modules{$main::task->{TASK}}->{DIR};}
    $main::alignTaskRoutines{$main::task->{TASK}}($main::SCRIPT, $stdout, \@main::alignTasks, $run);
  }
  script_tools::copyFiles($main::SCRIPT, 1);
  script_tools::finishScript(
    $main::SCRIPT,
    "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}",
    $main::runFileName,
    "align"
  );

  $bam = "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/$dir/$main::task->{FILE}";
  return $bam;
}

# If a completed version of the merged files exists in the specified bam path,
# from a previous increment, copy the file into the current directory structure
# and rename to be from the current increment.
sub transferMergedBam {
  my $stdout = $_[0];

  $main::SCRIPT = script_tools::createScript($main::mergeFileName, "Merge", 1, "bigmem");
  script_tools::scriptFail($main::SCRIPT, $main::mergeFileName);
  script_tools::transferFiles($main::SCRIPT);

  print $main::SCRIPT ("# Copy the previous increment bam file to the local directory.\n\n");
  print $main::SCRIPT ("INPUT_DIR=$main::sampleInfo{$stdout}->{PATH}\n");
  print $main::SCRIPT ("OUTPUT_DIR=$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/merged\n");
  print $main::SCRIPT ("INPUT=$main::sampleInfo{$stdout}->{FILE}\nOUTPUT=$main::mergeFileName.bam\n\n");
  print $main::SCRIPT ("TransferFiles \$INPUT_DIR \$OUTPUT_DIR/ \$INPUT \$OUTPUT\n\n");
  $main::task->{PATH} = "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/merged";
  $main::task->{FILE} = "$main::mergeFileName.bam";
  script_tools::copyFiles($main::SCRIPT, 1);
  script_tools::finishScript(
    $main::SCRIPT,
    "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}",
    $main::MergeFileName,
    "merge"
  );
  push(@main::completedBamFiles, "$main::task->{PATH}/$main::task->{FILE}");
  $main::sampleInfo{$stdout}->{COMPLETED_BAM} = "$main::task->{PATH}/$main::task->{FILE}";
}

# Create the necessary merge scripts.
sub createMergeScript {
  my $stdout = $_[0];

  %main::retainFiles = ();
  %main::deleteFiles = ();
  $main::renameFile  = "$main::mergeFileName.bam";
  $main::modules{RENAME_BAM}->{DIR} = "merged";

# Reset the main task structure.
  $main::task = {
    TASK         => $main::mergeTasks[0],
    TASKID       => 0,
    LOCATION     => "local",
    PATH         => $main::sampleInfo{$stdout}->{PATH},
    FILE         => $main::sampleInfo{$stdout}->{FILE},
    RETAIN_INPUT => "no"
  };

  $main::SCRIPT = script_tools::createScript($main::mergeFileName, "Merge", 1, "bigmem");
  script_tools::scriptFail($main::SCRIPT, $main::mergeFileName);
  script_tools::transferFiles($main::SCRIPT);
  while ($main::task->{TASK} ne "Complete") {
    $main::mergeTaskRoutines{$main::task->{TASK}}($main::SCRIPT, $stdout, \@main::mergeTasks);
  }
  script_tools::copyFiles($main::SCRIPT, 1);
  script_tools::finishScript(
    $main::SCRIPT,
    "$main::outputDirectory/$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}",
    $main::mergeFileName,
    "merge"
  );
  push(@main::completedBamFiles, "$main::task->{PATH}/$main::task->{FILE}");
  $main::sampleInfo{$stdout}->{COMPLETED_BAM} = "$main::task->{PATH}/$main::task->{FILE}";
}

# Print some statistics about the created scripts to screen.
sub printScriptStatistics {
  my $plural;

# Print to screen some information about the discovered bam files.
  my $numberFoundBams = (scalar(@main::currentIncrementBams)  +
                         scalar(@main::previousIncrementBams) +
                         scalar(@main::previousBams));
  if ($numberFoundBams != 0) {print("Existing merged bam files include:\n");}

  if (scalar(@main::currentIncrementBams) != 0) {
    $plural = (scalar @main::currentIncrementBams == 1) ? "" : "s";
    print("\t",scalar(@main::currentIncrementBams),"\tbam file$plural from the current increment.\n");
  }
  if (scalar(@main::previousIncrementBams) != 0) {
    $plural = (scalar @main::previousIncrementBams == 1) ? "" : "s";
    print("\t",scalar(@main::pPreviousIncrementBams),"\tbam file$plural from the previous increment.\n");
  }
  if (scalar(@main::previousBams) != 0) {
    $plural = (scalar @main::previousBams == 1) ? "" : "s";
    print("\t",scalar(@main::pPreviousBams),"\tbam file$plural from a previous date.\n");
  }
  if ($numberFoundBams != 0) {print("\n");}

  print("Merge script statistics:\n");
  $plural = (scalar @main::numberTotalMerge == 1) ? "" : "s";
  print("\t$main::numberTotalMerge\ttotal merged bam file$plural\n");

  $plural = (scalar @main::numberComplete == 1) ? "" : "s";
  print("\t$main::numberComplete\texisting bam file$plural requiring no modification.\n");

  $plural = (scalar @main::numberWithdrawn == 1) ? "" : "s";
  print("\t$main::numberWithdrawn\texisting bam file$plural with read groups withdrawn or modified.\n");

  $plural = (scalar @main::numberAdd == 1) ? "" : "s";
  print("\t$main::numberAdd\texisting bam file$plural with read groups added.\n");

  $plural = (scalar @main::numberRemoved == 1) ? "" : "s";
  print("\t$main::numberRemoved\texisting bam file$plural removed from pipeline.\n");

  $plural = (scalar @main::numberCreated == 1) ? "" : "s";
  print("\t$main::numberCreated\tbam file$plural to be created.\n\n");

# Write to file the missing fastq files and write to screen the sample x technology
# pairs that are incomplete.
  missingFastq();
}

# If a run is missing files, write to a warnings file and remove this run
# from AlignmentInfo.
sub missingFastq {
  open(OUT,">missingFastq.txt");
  foreach my $stdout (sort keys %main::missingFastq) {
    print OUT ("$stdout\n");
    foreach my $fastq (@{$main::missingFastq{$stdout}}) {
      print OUT ("\t$fastq\n");
    }
  }
  close(OUT);
  if (! -s "missingFastq.txt") {
    unlink("missingFastq.txt");
  } else {
    print("WARNING: Some fastq files were missing.  Check missingFastq.txt\n");
  }
}

1;
