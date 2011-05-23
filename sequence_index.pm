#!/usr/bin/perl -w

package sequence_index;

use strict;
use Cwd 'abs_path';

# If the -seqold option was specified, read through the old
# sequence.index file and populate a hash table with the md5sums
# of the fastq files.

sub parsePreviousIndexFile {
  $main::previousIndex = abs_path($main::previousIndex);
  open(SEQIND,"<$main::previousIndex") || noIndex();
  print("Reading index file ($main::previousIndex)...");
  while(<SEQIND>) {
    chomp;
    my @seqinfo   = split(/\t/, $_);
    my $fastq1    = $seqinfo[0];
    my $fastq2    = $seqinfo[19];
    $fastq1       = (split(/\//, $fastq1))[-1];
    my $md5       = $seqinfo[1];
    my $run_id    = $seqinfo[2];
    my $sample    = $seqinfo[9];
    my $read_type = $seqinfo[18];

    if (! defined $fastq2 || $fastq2 eq "") {$read_type = "SINGLE";}
    my $withdrawn = $seqinfo[20];
    if ($withdrawn == 0 && ($fastq1 =~ /_1/ || $fastq1 !~ /_/)) {$main::md5Sum{"$sample.$run_id.$read_type"} = $md5;}
  }
  close(SEQIND);
  print("done.\n");
}

# Check if a sequence.index file is defined and if it is,
# check that it exists, then read in the information.  If
# no sequence.index file exists, terminate the script.
# Future development should allow fastq files to be
# interrogated for metadata and this remove the requirement
# for a sequence.index file.
sub parseIndexFile {
  $main::indexFile = abs_path($main::indexFile);
  open(SEQIND,"<$main::indexFile") || noIndex();
  print("Reading index file $main::indexFile...");
  while (<SEQIND>) {indexLine();}
  close(SEQIND);
  print("done.\n");

  # Clear the hash containing the md5sums as this is no longer needed,
  %main::md5Sum=();
}

# NoSeqIndex is called when the sequence.index file is not present.
# Currently this causes the program to exist with an error message,
# but may be extended if other metadata files are allowed.
sub noIndex {
  print("\n\n***SCRIPT TERMINATED***\n");
  print("The sequence index file \"$main::indexFile\" could not be found.\n");
  die("Error in sequenceIndex::noIndex\n");
}

# Read the metadata contained in the sequence.index file.
sub indexLine {
  chomp;
  my @seqinfo = split(/\t/, $_);

  # Check if the run has been withdrawn.  If so, print a warning to file
  # and do not add this run to AlignmentInfo.
  my $withdrawn = $seqinfo[20];
  if ($withdrawn == 0 && $seqinfo[0] !~ /FASTQ_FILE/) {

# Check that the sample from this line of the sequence.index
# file is one of the samples selected for running through the
# pipeline.  If so, perform the remaining operations.
    my $sample = $seqinfo[9];

# If no samples file was provided, add this sample to $main::Samples.
    if (! defined $main::SamplesFile) {$main::samples{$sample} = 1;}
    if (exists $main::samples{$sample}) {

# Collect the relevant data for this run.
      my $fastq1     = $seqinfo[0];
      my $fastq2     = $seqinfo[19];
      $fastq1        = (split(/\//, $fastq1))[-1];
      $fastq2        = (split(/\//, $fastq2))[-1];
      my $runid      = $seqinfo[2];
      my $technology = $seqinfo[12];
      my $read_type  = $seqinfo[18];
      if (! defined $fastq2 || $fastq2 eq "") {$read_type = "SINGLE";}

# Make sure that the technology is in the form ILLUMINA, 454 or SOLID.
      if ($technology =~ m/illumina/i) {$technology = "illumina";}
      elsif ($technology =~ m/454/i) {$technology = "454";}
      elsif ($technology =~ m/solid/i) {$technology = "solid";}
      else {
        print("\n\n***SCRIPT TERMINATED***\n");
        print("Unknown technology: $technology.\n");
        die("Error in sequenceIndex::indexLine.\n");
      }

# Now extract the remaining relevant information.
      my $md5 = $seqinfo[1];
      if ($md5 eq "") {$md5 = "undefined";}
      my $study_id = $seqinfo[3];
      if ($study_id eq "") {$study_id = "undefined";}
      my $population = $seqinfo[10];
      if ($population eq "") {$population = "undefined";}
      my $centre = $seqinfo[5];
      if ($centre eq "") {$centre = "undefined";}
      my $platform = $seqinfo[12];
      if ($platform eq "") {$platform = "undefined";}
      my $library = $seqinfo[14];
      if ($library eq "") {$library = "undefined";}
      my $run_name = $seqinfo[15];
      if ($run_name eq "") {$run_name = "undefined";}
      my $frag_length = $seqinfo[17];
      if ($frag_length eq "") {$frag_length = "undefined";}
      my $read_count = $seqinfo[23];
      my $bp_count = $seqinfo[24];

# Check that the sample belongs to the correct population.  If this is the
# first time this sample has been encountered, create a hash element
# associating this sample with a population.  If the sample has already
# been associated, check that it corresponds to the same population as
# previous files for this sample.  If a difference is encountered, terminate
# the script with an informative warning.
      if (! defined $main::samplePopulation{$sample}) {$main::samplePopulation{$sample} = $population;}
      else {
        if ($population ne $main::samplePopulation{$sample}) {
          print("\n\n***SCRIPT TERMINATED***\n");
          print("Error with population when processing entry:\t$sample.$runid ($fastq1)\n\n");
          print("Population associated with the entry:\t$population\n");
          print("Population previously associated with this sample:\t$main::samplePopulation{$sample}\n");
          die("Error in sequenceIndex::indexLine.\n");
        }
      }

# Populate the data structure SampleInfo.  This hash should contain a string for each
# sample/technology pair and contain information on which runs are supposed to
# be included.
#
# The tag status indicates that the merged bam file should be included in the pipeline.
# This could either mean that it contains all of the information for this sample x technology
# pair, or that some of the runs have been removed, amended ot require merging with the
# extant merged bam file.  The file is assumed to be included unless found otherwise later
# (for instance if all read groups require removal). Status can take the following values:
#
# 1. remove
# 2. withdraw
# 3. add
# 4. complete
# 5. create
#
# See MergeBamTools::DetermineMergeBamStatus for more details.
#
# Also set the flag HASFASTQ to false.  In create_scripts::itialise, a search for
# existing fastq files for each sample will be performed and this will be set to
# true if any exist.
      my $stdout = "$sample.mapped.$technology.$main::aligner.$population.$main::date";
      if (defined $main::exome) {$stdout =~ s/$main::date$/exome\.$main::date/;}
      if (! exists $main::sampleInfo{$stdout}) {
        $main::sampleInfo{$stdout} = {
          SAMPLE     => $sample,
          DATE       => $main::date,
          POPULATION => $population,
          TECHNOLOGY => $technology,
          STUDY      => $study_id,
          STATUS     => "create",
          PATH       => "",
          FILE       => "",
          INCREMENT  => "",
          ALIGN      => "yes",
          RUN        => {$runid, "1"},
          HASFASTQ   => "false"
        };
      }
      else {$main::sampleInfo{$stdout}->{RUN}{$runid} = 1;}

# Now populate the data structre RunInfo.  This contains information specific to
# each sample run (single and paired end runs are handled in separate entries).

      if (! exists $main::runInfo{$runid}) {
        $main::runInfo{$runid} = {
          SAMPLE     => $sample,
          TECHNOLOGY => $technology,
          CENTRE     => $centre,
          RUN_NAME   => $run_name,
          PLATFORM   => $platform,
          LIBRARY    => $library,
          READCOUNT  => $read_count,
          BPCOUNT    => $bp_count,
          STATUS     => "align"
        };
      }
      if ($read_type eq "SINGLE") {
        $main::runInfo{$runid}->{FASTQ} = $fastq1;
      }
      elsif ($read_type eq "PAIRED") {
        if ($fastq1 =~ /_1/) {
          $main::runInfo{$runid}->{FRAGMENT} = $frag_length;
          $main::runInfo{$runid}->{FASTQ1}   = $fastq1;
          $main::runInfo{$runid}->{FASTQ2}   = $fastq2;
        }
      }
      else {
        print("\n$stdout: Unknown read type (single- or paired-end: $read_type).\n");
        print("Omitted from the pipeline.\n");
      }

# Check if the md5sum has changed from the previous increment (if using the incremental pipeline).
      my $key = "$sample.$runid.$read_type";
      if (defined $main::previousDate && defined $main::md5Sum{$key}) {
        if ($read_type eq "SINGLE" && $main::md5Sum{$key} ne $md5) {
          $main::runInfo{$runid}->{STATUS} = "realign";
        }
        elsif ($read_type eq "PAIRED" && $fastq1 =~ /_1/ && $main::md5Sum{$key} ne $md5) {
          $main::runInfo{$runid}->{STATUS} = "realign";
        }
      }
    }
  }
}

# As an alternative to a sequence index file, a different format or
# input meta data can be used.  If this is the case, read in the
# information from this meta data file.
sub metaData {
  open(META,"<$main::metaData") || noMetaData();
  print("Reading index file $main::metaData...");

  my $sampleID = 1;
  while (<META>) {
    my $sample         = "SM.$sampleID";
    my $technology     = "";
    my $library        = "";
    my $readLength     = 0;
    my $readCount      = 0;
    my $bpCount        = 0;
    my $population     = "";
    my $fragmentLength = 0;
    my $readType       = "SINGLE";
    my $runid          = "";
    my $fastq1         = "";
    my $fastq2         = "";

    chomp;
    my @info = split("\t", $_);

# Search through the supplied information.
    for (my $i = 0; $i < @info; $i++) {
      my $tag   = (split(":", $info[$i]))[0];
      my $value = (split(":", $info[$i]))[1];

      if ($tag eq "SM") {$sample = $value;}
      elsif ($tag eq "ST") {$technology = $value;}
      elsif ($tag eq "LB") {$library = $value;}
      elsif ($tag eq "RL") {
        $readLength = $value;

# Since the sequence.index file gave the read count and the base pair count,
# the read length information is calculated elsewhere based on these values.
# Thus, define abritrary values for the read count and base count that will
# result in the correct read length.
        $readCount = 1;
        $bpCount   = $readLength;
      }
      elsif ($tag eq "POP") {$population = $value;}
      elsif ($tag eq "FL") {$fragmentLength = $value;}
      elsif ($tag eq "FQ") {
        my $fastqString = $value;
        if ($fastqString =~ /^(\S+)\s+(\S+)$/) {
          $readType = "PAIRED";
          $fastq1 = $1;
          $fastq2 = $2;
        } elsif ($fastqString =~ /^(\S+)$/) {
          $fastq1 = $1;
        } else {
          print("\n***SCRIPT TERMINATED***\n");
          print("Fastq entry must have one of the following forms:\n\tFQ:fastq\n\tFQ:fastq1 fastq2\n\n");
          print("Actual value:\n\t$fastqString\n");
          die("Error in sequenceIndex::metaData.\n");
        }
        $runid = (split(/\./, $fastq1))[0];
      }
    }

# Check for undefined values.
    if ($sample eq "SM.$sampleID") {$sampleID++;}
    if ($technology eq "") {
      print("\n***SCRIPT TERMINATED***\n");
      print("No technology defined for $sample.\n");
      die("Error in sequenceIndex::metaData.\n");
    }
    if ($fastq1 eq "") {
      print("\n***SCRIPT TERMINATED***\n");
      print("At least one fastq file must be provided with sample, $sample.\n");
      die("Error in sequenceIndex::metaData.\n");
    }

    my $stdout="$sample.$technology.$main::aligner.$population.$main::date";
    if (! exists $main::sampleInfo{$stdout}) {
      $main::sampleInfo{$stdout} = {
        SAMPLE     => $sample,
        DATE       => $main::date,
        POPULATION => $population,
        TECHNOLOGY => $technology,
        STUDY      => "",
        STATUS     => "create",
        PATH       => "",
        FILE       => "",
        INCREMENT  => "",
        ALIGN      => "yes",
        RUN        => {$runid,"1"}
      };
    }
    else {$main::sampleInfo{$stdout}->{RUN}{$runid} = 1;}

# Now populate the data structre RunInfo.  This contains information specific to
# each sample run (single and paired end runs are handled in separate entries).
    if (! exists $main::runInfo{$runid}) {
      $main::runInfo{$runid} = {
        SAMPLE     => $sample,
        TECHNOLOGY => $technology,
        CENTRE     => "",
        RUN_NAME   => "",
        PLATFORM   => "",
        LIBRARY    => $library,
        READCOUNT  => $readCount,
        BPCOUNT    => $bpCount,
        STATUS     => "align"
      };
    }
    if ($readType eq "SINGLE") {
      $main::runInfo{$runid}->{FASTQ} = $fastq1;
    } elsif ($readType eq "PAIRED") {
      if ($fastq1 =~ /_1/) {
        $main::runInfo{$runid}->{FRAGMENT} = $fragmentLength;
        $main::runInfo{$runid}->{FASTQ1}   = $fastq1;
        $main::runInfo{$runid}->{FASTQ2}   = $fastq2;
      }
    } else {
      print("\n$stdout: Unknown read type (single- or paired-end: $readType).\n");
      print("Omitted from the pipeline.\n");
    }
  }

  close(META);
  print("done.\n");
}

# NoMetaDatax is called when the meta data file is not present.
sub noMetaData {
  print("\n\n***SCRIPT TERMINATED***\n");
  print("The meta data file \"$main::metaData\" could not be found.\n");
  die("Error in sequenceIndex::noMetaData.\n");
}

1;
