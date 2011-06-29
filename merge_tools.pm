#!/usr/bin/perl -w

package merge_tools;

use strict;

# Using the SampleInfo hash, count the number of samples and sample x technology
# pairs that are to be analysed by the pipeline.

sub countSamples {
  my %uniqueSamples            = ();
  my %uniqueSampleXtechnology  = ();
  my %uniqueTechnologies       = ();
  my $numberSamples            = 0;
  my $plural                   = "";

  foreach my $stdout (keys %main::sampleInfo) {
    my $sample     = $main::sampleInfo{$stdout}->{SAMPLE};
    my $technology = $main::sampleInfo{$stdout}->{TECHNOLOGY};
    if (! exists $uniqueSamples{$sample}) {
      $uniqueSamples{$sample} = 1;
      $numberSamples++;
    }
    if (! exists $uniqueSampleXtechnology{"$sample.$technology"}) {
      $uniqueSampleXtechnology{"$sample.$technology"} = 1;
      $main::numberStdout++;
    }
    if ($technology =~ /illumina/i) {$uniqueTechnologies{"illumina"}++;}
    elsif ($technology =~ /454/i) {$uniqueTechnologies{"454"}++;}
    elsif ($technology =~ /solid/i) {$uniqueTechnologies{"solid"}++;}
    else {
      print("\n***SCRIPT TERMINATED***\n\n");
      print("Error in MergeBamTools::CountSamples.\n");
      print("Unknown sequencing technology: $technology.\n");
      die("Error in merge_tools::countSamples.\n");
    }
  }

# Clear the hashes used to ensure uniqueness.
  %uniqueSamples           = ();
  %uniqueSampleXtechnology = ();
  if (! exists $uniqueTechnologies{"illumina"}) {$uniqueTechnologies{"illumina"} = 0;}
  if (! exists $uniqueTechnologies{"454"}) {$uniqueTechnologies{"454"} = 0;}
  if (! exists $uniqueTechnologies{"solid"}) {$uniqueTechnologies{"solid"} = 0;}

  if ($numberSamples == 1) {$plural = "";}
  else {$plural = "s";}
  print("\nPipeline will analyse:\n\t$numberSamples\tunique sample$plural.\n");

  $plural = ($main::numberStdout == 1) ? "" : "s";
  print("\t$main::numberStdout\tunique sample x technology pair$plural.\n\n");

  $plural = ($main::uniqueTechnologies{"illumina"} == 1) ? "" : "s";
  print("\t$uniqueTechnologies{\"illumina\"}\tsample$plural with illumina data.\n");

  $plural = ($main::uniqueTechnologies{"454"} == 1) ? "" : "s";
  print("\t$uniqueTechnologies{\"454\"}\tsample$plural with 454 data.\n");

  $plural = ($main::uniqueTechnologies{"solid"} == 1) ? "" : "s";
  print("\t$uniqueTechnologies{\"solid\"}\tsample$plural with solid data.\n\n");
}

# Select which merged bam to carry forward.
sub selectMergedBam {
  foreach my $stdout (keys %main::sampleInfo) {
    my $bamFound   = 0;
    my $sample     = $main::sampleInfo{$stdout}->{SAMPLE};
    my $technology = $main::sampleInfo{$stdout}->{TECHNOLOGY};

# Check for bam files for the selected date.
    $bamFound = searchBams($stdout, $sample, $technology, $bamFound, "current", \@main::currentIncrementBams);

# If a file for the current date wasn't found and a previous increment is defined,
# search for a bam file from this date.
    if ($bamFound == 0 && defined $main::previousDate) {
      $bamFound = searchBams($stdout, $sample, $technology, $bamFound, "previous", \@main::previousIncrementBams);
    }

# If no merged bam files have been found for either the current or the previous
# increment date, check for merged bam files from arbitrary dates.
    if ($bamFound == 0) {$bamFound = searchBams($stdout, $sample, $technology, $bamFound, "neither", \@main::previousBams);}

    my $path      = general_tools::findTag($main::mergeInfo{$stdout}{"info"},"path");
    my $file      = general_tools::findTag($main::mergeInfo{$stdout}{"info"},"file");
    my $increment = general_tools::findTag($main::mergeInfo{$stdout}{"info"},"increment");
  }
}

# Search the given array for merged bam files and fail if multiple are found.
sub searchBams {
  my $stdout     = $_[0];
  my $sample     = $_[1];
  my $technology = $_[2];
  my $bamFound  = $_[3];
  my $text       = $_[4];
  my @bamFiles  = @{$_[5]};

  foreach my $fullFile (@bamFiles) {
    if ($fullFile =~ /$sample/ && $fullFile =~ /$technology/i) {

# Check to see if there is already a merged bam file associated with this sample x
# technology pair.  If so, terminate the script with an error message.  There is
# currently no way for the pipeline to determine which files to include of there
# are multiple files (except if the differences are in the date).
      if ($bamFound == 1) {failSelectMergedam($stdout, $fullFile);}
      my $file = (split(/\//, $fullFile))[-1];
      (my $path = $fullFile ) =~ s/\/$file//;
      $main::sampleInfo{$stdout}->{PATH}      = $path;
      $main::sampleInfo{$stdout}->{FILE}      = $file;
      $main::sampleInfo{$stdout}->{INCREMENT} = $text;
      $bamFound=1;
    }
  }

  return $bamFound;
}

# Terminate the script if selecting a merged bam file fails.
sub failSelectMergedBam {
  my $stdout=$_[0];
  my $fullFile=$_[1];

  my $sample     = $main::sampleInfo{$stdout}->{SAMPLE};
  my $technology = $main::sampleInfo{$stdout}->{TECHNOLOGY};
  my $path       = $main::sampleInfo{$stdout}->{PATH};
  my $file       = $main::sampleInfo{$stdout}->{FILE};

  print("Multiple merged bam files exist for a $sample/$technology.\n");
  print("There is currently no way for the pipeline to choose between the following files:\n");
  print("\t$path/$file\n\t$fullFile\n");
  die("Error in merge_tools::failSelectMergedBam.\n");
}

# For all sample x technology pairs that have an associated merged bam file, this
# file can be interrogated for the items discussed prior to calling this routine 
# in Pipeline.pl.

sub interrogateMergedBamFile {
  foreach my $stdout (keys %main::sampleInfo) {
    my $path = $main::sampleInfo{$stdout}->{PATH};
    if ($path eq "") {$main::sampleInfo{$stdout}->{STATUS}="create";}
    else {
      my $file = $main::sampleInfo{$stdout}->{FILE};
      my %RG = bamtools::GetReadGroups("$path/$file");
      verifyAllRGsExist($stdout, \%RG);
      checkForWithdrawnRG($stdout, \%RG);
      determineMergedBamStatus($stdout);
    }
  }
}

# Check that the merged bam file contains all the read group ids that it should
# (i.e. are listed in the hash %main::MergeInfo.
sub verifyAllRGsExist {
  my $stdout = $_[0];
  my %RG     = %{$_[1]};

# Check that all of the runs in the index file are contained in the merged bam
# file.  If the run has been marked for realignment due to an altered md5sum, 
# tag the run as requiring alignment.  Similarly for runs not found in the RG
# hash.  
#
# For required runs that are found in te merged bam file, set the tag to complete
# to indicate that these runs do not require alignment.
  foreach my $RG (keys %{$main::sampleInfo{$stdout}->{RUN}}) {

# Check that this run id is unique to this sample x technology pair.
    if ($main::sampleInfo{$stdout}->{SAMPLE} ne $main::runInfo{$RG}->{SAMPLE}) {
      print("\n***SCRIPT TERMINATED***\n\n");
      print("Run $RG is associated with more thatn one sample.\n");
      die("Terminated in merge_tools::verifyAllRGsExist.\n");
    }
    if ($main::sampleInfo{$stdout}->{TECHNOLOGY} ne $main::runInfo{$RG}->{TECHNOLOGY}) {
      print("\n***SCRIPT TERMINATED***\n\n");
      print("Run $RG is associated with more thatn one technology.\n");
      die("Terminated in merge_tools::verifyAllRGsExist.\n");
    }

# Remove the single or paired end tag from the read group name.
    if ($main::runInfo{$RG}->{STATUS} eq "realign") {}
    elsif (exists $RG{$RG}) {$main::runInfo{$RG}->{STATUS} = "complete";}
    else {$main::runInfo{$RG}->{STATUS} = "align";}
  }
}

# Now check that there are no read groups in the merged bam file that are not
# in the MergeInfo hash table.  If there are, these are withdrawn runs and
# should be removed from the bam file,
sub checkForWithdrawnRG {
  my $stdout = $_[0];
  my %RG     = %{$_[1]};

  foreach my $RG (keys %RG) {
    if (! exists $main::runInfo{$RG}) {
      $main::runInfo{$RG}->{STATUS} = "withdrawn";
      $main::sampleInfo{$stdout}->{RUN}{$RG} = 1;
    }
  }
}

# Determine the status of the merged bam file.  For example, does it need to be
# removed (all RG are withdrawn or require remerging due to changed md5sums),
# modified (some of the RGs remain unchanged, but some may need removal,
# realigning or adding) or included as is (i.e. nothing needs removing or
# realigning and no new runs need adding).
sub determineMergedBamStatus {
  my $stdout          = $_[0];
  my $numberRuns      = 0;
  my $numberComplete  = 0;
  my $numberAlign     = 0;
  my $numberRealign   = 0;
  my $numberWithdrawn = 0;

# Cycle through the list of runs for this sample x technology pair and count the
# total number of runs and the number of the following allowed run statuses:
#
# 1. complete - is in the merged bam file and the md5sum has not been modified.
# 2. align - is not in the merged bam file so requires alignment.
# 3. realign - is in the bam file, but the md5sum has changed requiring realignment.
# 4. withdrawn - is in the bam file, but should be removed.
  foreach my $RG (keys %{$main::sampleInfo{$stdout}->{RUN}}) {
    my $status = $main::runInfo{$RG}->{STATUS};
    if ($main::runInfo{$RG}->{STATUS} eq "complete") {$numberComplete++;}
    elsif ($main::runInfo{$RG}->{STATUS} eq "align") {$numberAlign++;}
    elsif ($main::runInfo{$RG}->{STATUS} eq "realign") {$numberRealign++;}
    elsif ($main::runInfo{$RG}->{STATUS} eq "withdrawn") {$numberWithdrawn++;}
    else {
      my $sample = $main::sampleInfo{$stdout}->{SAMPLE};
      print("\n***SCRIPT TERMINATED***\n\n");
      print("Unknown status for run $RG in sample $main::sampleInfo{$stdout}->{SAMPLE} ");
      print("(status=$main::runInfo{$RG}->{STATUS})\n");
      print("Error ocurred in merge_tools::determineMergedBamStatus.\n");
      exit(1);
    }
    $numberRuns++;
  }

# Now determine the status of the merged bam file.  The status can take the following values:
#
# 1. remove - the merged bam file can be removed and is not carried forward in the pipeline.
# 2. withdraw - some read groups require removal (either as they have been withdrawn, or the
# fastq files have been modified and they need to be realigned).
# 3. add - the contained read groups have not been withdrawn or modified, but new read groups
# need to be added.  The merged bam file is thus carried forward as is to be merged with the
# new alignments,
# 4. complete - the contained read groups have not been withdrawn or modified and no new read
# groups need to be added.  This sample x technology pair is essentially complete and no 
# further action is require from the pipeline.
# 5. create - the original classification will remain if no bam file exists and this
# routine is not executed.  In this case, there is no merged bam file and so one needs to be
# created.
# 6. process - this value is not set in this routine, however, for the case of a bam file that
# is not marked as complete, all duplicate mark tags will be stripped out of the bam file
# and so it will require post merging steps to be performed.  Thus after, removing these tags,
# the bam file status will be labelled as process to indicate that post merge processing is
# required.
#
# The merged bam file can be removed because there are no read groups listed as complete.
# This means that all the read groups in the existing merged bam file have been 
# withdrawn or require realignment.
  if ($numberComplete == 0) {
    if ($numberAlign != 0 || $numberRealign != 0) {$main::sampleInfo{$stdout}->{STATUS}="add";}
    else {$main::sampleInfo{$stdout}->{STATUS}="remove";}
    $main::numberRemoved++;
  }

# The merged bam file is to be retained, but requires modification.  This will be true if
# there are some read groups listed as complete, but also some listed as requiring
# realignment or withdrawal.
  elsif ($numberComplete != 0 && ($numberWithdrawn != 0 || $numberRealign != 0)) {
    $main::sampleInfo{$stdout}->{STATUS}="withdraw";
    $main::numberWithdrawn++;
  }

# The merged bam file just requires new read groups adding - no modification.
  elsif ($numberWithdrawn == 0 && $numberRealign == 0 && $numberAlign != 0) {
    $main::sampleInfo{$stdout}->{STATUS}="add";
    $main::numberAdd++;
  }

# The merged bam is complete and requires no modification of existing read groups or the addition
# of any new read groups.
  elsif ($numberComplete == $numberRuns) {
    $main::sampleInfo{$stdout}->{STATUS} = "complete";
    $main::numberComplete++;
    push(@main::completedBamFiles, "$main::sampleInfo{$stdout}->{PATH}/$main::sampleInfo{$stdout}->{FILE}");
  }

# Cause an exception if none of the above cases are true.
  else {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("Error in MergeBamFile::DetermineMergedBamStatus\n");
    print("Unable to determine the status of the merged bam file.\n");
    die("Error in merge_tools::determineMergedBamStatus.\n");
  }

# Finally add a tag (align) that indicates whether any of the read groups require aligning.
  if ($numberAlign != 0 || $numberRealign != 0) {$main::sampleInfo{$stdout}->{ALIGN} = "yes";}
  else {$main::sampleInfo{$stdout}->{ALIGN} = "no";}
}

1;
