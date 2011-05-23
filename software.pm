#!/usr/bin/perl -w

package software;

use strict;

# Given the requested aligner, set up all of the necessary files that are required
# and define the steps to follow in the alignment process.  This includes alignment
# of individual fastq files as well as merging sample level bam files and any
# associated processing tools (base quality recalibration, duplicate marking etc.).
sub aligners {
  #foreach my $module (keys %main::ModuleInfo) {
  #  my $command="$main::ModuleInfo{$module}->{BIN}/$main::ModuleInfo{$module}->{COMMAND}";
  #  if ($main::ModuleInfo{$module}->{COMMAND} ne "") {push(@main::CheckExistence,$command);}
  #}

# If alignment with Mosaik, define the necessary pipeline.
  if ($main::aligner eq "mosaik") {
    reference::mosaikReferenceFiles();

    # Define the Mosaik pipeline.  Start with elements common to both
    # Mosaik versions 1 and 2.
    @main::alignTasks = ("FASTQVALIDATOR");

    # If Mosaik v1 is being used, include the sort and text components.
    if (! defined $main::mosaikVersion2) {
      push(@main::alignTasks, "MOSAIKBUILDV1");
      push(@main::alignTasks, "MOSAIKALIGNERV1");
      push(@main::alignTasks, "MOSAIKSORT");
      push(@main::alignTasks, "MOSAIKTEXT");

    # Mosaik version 2 requires an additional sort for all of the
    # produced bam files.
    } else {
      push(@main::alignTasks, "MOSAIKBUILDV2");
      push(@main::alignTasks, "MOSAIKALIGNERV2");
      push(@main::alignTasks, "SORT_MOSAIKV2");
    }

    # More common tasks.
    push(@main::alignTasks, "INDEX");
    push(@main::alignTasks, "BQ_RECALIBRATION");
    push(@main::alignTasks, "RENAME_BAM");
    push(@main::alignTasks, "INDEX");

    # Now build a hash table that defines the subroutines to execute
    # these tasks.
    %main::alignTaskRoutines = (
      FASTQVALIDATOR   => \&tools::fastQValidator,
      MOSAIKBUILDV1    => \&mosaik::mosaikBuild,
      MOSAIKBUILDV2    => \&mosaik::mosaikBuild,
      MOSAIKALIGNERV1  => \&mosaik::mosaikAligner,
      MOSAIKALIGNERV2  => \&mosaik::mosaikAligner,
      MOSAIKSORT       => \&mosaik::mosaikSort,
      MOSAIKTEXT       => \&mosaik::mosaikText,
      SORT_MOSAIKV2    => \&bamtools::sortMosaikv2Bam,
      BQ_RECALIBRATION => \&tools::baseQualityRecalibration,
      RENAME_BAM       => \&tools::renameBam,
      INDEX            => \&bamtools::index
    );

# Set up the Mosaik parameters.
    mosaik::mosaikParameters();

# BWA.
  } elsif ($main::aligner eq "bwa") {
    $main::aligners{"bwa"}="/share/software/bwa/bwa-0.5.8a";

# No aligner.
  } elsif ($main::Aligner =~ /none/i) {
  }

  else {command_line::checkAligner();}
}

# Define the pipeline for merging together the run level bam files into
# sample x technology level bam files and perform additional post-
# processing tasks.
sub mergePipeline {
  $main::dbsnpBin = "/d2/data/references/build_37/dbsnp";
  $main::dbsnp    = "dbsnp_129_b37.rod";

  general_tools::checkFileExists("$main::dbsnpBin/$main::dbsnp");

  # Define the merge pipeline.
  @main::mergeTasks = (
    "MODIFY_BAM",
    "MERGE_BAM",
    "INDEX",
    "DUPLICATE_MARK_PICARD",
    "DUPLICATE_MARK_BCM",
    "RENAME_BAM",
    "INDEX",
    "BAM_STATISTICS"
  );

  %main::mergeTaskRoutines = (
    MODIFY_BAM            => \&bamtools::modifyBam,
    MERGE_BAM             => \&bamtools::mergeBamFiles,
    DUPLICATE_MARK_PICARD => \&tools::duplicateMarkPicard,
    DUPLICATE_MARK_BCM    => \&tools::duplicateMarkBCM,
    RENAME_BAM            => \&tools::renameBam,
    INDEX                 => \&bamtools::index,
    BAM_STATISTICS        => \&bamtools::statistics
  );
}

1;
