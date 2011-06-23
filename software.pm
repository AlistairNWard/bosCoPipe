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
    if (!defined $main::noBQRecal) {push(@main::alignTasks, "BQ_RECALIBRATION");}
    push(@main::alignTasks, "RESOLVE");
    push(@main::alignTasks, "RENAME_BAM");
    if (defined $main::mosaikVersion2) {push(@main::alignTasks, "MOVE_MOSAIK2_BAM");}
    push(@main::alignTasks, "INDEX");

    # Now build a hash table that defines the subroutines to execute
    # these tasks.
    %main::alignTaskRoutines = (
      BQ_RECALIBRATION => \&tools::baseQualityRecalibration,
      FASTQVALIDATOR   => \&tools::fastQValidator,
      INDEX            => \&bamtools::index,
      MOSAIKBUILDV1    => \&mosaik::mosaikBuild,
      MOSAIKBUILDV2    => \&mosaik::mosaikBuild,
      MOSAIKALIGNERV1  => \&mosaik::mosaikAligner,
      MOSAIKALIGNERV2  => \&mosaik::mosaikAligner,
      MOSAIKSORT       => \&mosaik::mosaikSort,
      MOSAIKTEXT       => \&mosaik::mosaikText,
      MOVE_MOSAIK2_BAM => \&tools::moveMosaik2Bam,
      RENAME_BAM       => \&tools::renameBam,
      RESOLVE          => \&bamtools::resolve,
      SORT_MOSAIKV2    => \&bamtools::sortMosaikv2Bam
    );

  # Set up the Mosaik parameters.
    mosaik::mosaikParameters();

  # BWA.
  } elsif ($main::aligner eq "bwa") {
    $main::aligners{"bwa"}="/share/software/bwa/bwa-0.5.8a";

  # No aligner.
  } elsif ($main::Aligner =~ /none/i) {

  # If the aligner isn't recognised terminate,
  } else {
    command_line::checkAligner();
  }

  # Check that all of the required  software tools exist.
  foreach (@main::alignTasks) {
    if ($main::modules{$_}->{BIN}  ne "") {
      general_tools::checkFileExists("$main::modules{$_}->{BIN}/$main::modules{$_}->{COMMAND}");
    }
  }
}

# Define the pipeline for merging together the run level bam files into
# sample x technology level bam files and perform additional post-
# processing tasks.
sub mergePipeline {

  # Define the merge pipeline.
  @main::mergeTasks = (
    "MODIFY_BAM",
    "MERGE_BAM",
    "INDEX",
    "DUPLICATE_MARK_PICARD",
    "DUPLICATE_MARK_BCM",
    "RENAME_BAM",
    "INDEX",
    "BAM_STATISTICS",
    "MD5SUM"
  );

  %main::mergeTaskRoutines = (
    MODIFY_BAM            => \&bamtools::modifyBam,
    MERGE_BAM             => \&bamtools::mergeBamFiles,
    DUPLICATE_MARK_PICARD => \&tools::duplicateMarkPicard,
    DUPLICATE_MARK_BCM    => \&tools::duplicateMarkBCM,
    RENAME_BAM            => \&tools::renameBam,
    INDEX                 => \&bamtools::index,
    BAM_STATISTICS        => \&bamtools::statistics,
    MD5SUM                => \&tools::calculateMd5
  );

  # Check that all of the required  software tools exist.
  foreach (@main::mergeTasks) {
    if ($main::modules{$_}->{BIN}  ne "") {
      general_tools::checkFileExists("$main::modules{$_}->{BIN}/$main::modules{$_}->{COMMAND}");
    }
  }
}

# Now define SNP callers.
sub snpCallers {

  # freebayes.
  if ($main::snpCaller =~ /^freebayes$/i) {
    @main::snpCallTasks = (
      "FREEBAYES"
    );

    %main::snpCallRoutines = (
      FREEBAYES => \&freebayes::freebayes
    );

  # If the SNP caller isn't recognised, terminate.
  } else {
    command_line::checkSnpCaller();
  }

  # Check that all of the required  software tools exist.
  foreach (@main::snpCallTasks) {
    if ($main::modules{$_}->{BIN}  ne "") {
      general_tools::checkFileExists("$main::modules{$_}->{BIN}/$main::modules{$_}->{COMMAND}");
    }
  }
}

1;
