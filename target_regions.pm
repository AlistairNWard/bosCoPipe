#!/usr/bin/perl -w

package target_regions;

use strict;

# If a delimiter has not been specified, set the default.
sub snpDelimiter {
  if (!defined $main::snpDelimiter) {$main::snpDelimiter = ":";}
}

sub defineRegions {
  @target_regions::targetRegions = ();

  # If the user has specified that the genome should be broken
  # up by chromosome, but further division has not been specified,
  # assume that each chromosome is to be run in its entirety.
  if ($main::divideGenome eq "c" && !defined $main::targetRegionSize) {$main::targetRegionSize = 0;}

  # If no information on how to break up the genome is provided,
  # use the default of calling on 100kbp chunks on each chromosome.
  if (!defined $main::divideGenome) {
    $main::divideGenome = "c";
    if (!defined $main::targetRegionSize) {$main::targetRegionSize = 100;}
  }

  # Split the genome up into chromosomes for variant calling.
  if ($main::divideGenome eq "c") {

    # If the whole genomes are to be called on in a single run.
    if ($main::targetRegionSize == 0) {
      if (defined $main::referenceSequence) {
        $target_regions::targetRegions[0] = $main::referenceSequence;
      } else { 
        for (my $chr = 1; $chr < 23; $chr++) {$target_regions::targetRegions[$chr - 1] = $chr;}
        $target_regions::targetRegions[22] = "X";
        $target_regions::targetRegions[23] = "Y";
      }

      # Include the genome coordinates for each chromosome.
      for (my $chr = 0; $chr < @target_regions::targetRegions; $chr++) {
        my $extent = getChromosomeExtents($target_regions::targetRegions[$chr]);
        $target_regions::targetRegions[$chr] = "$target_regions::targetRegions[$chr]:1-$extent";
      }

    # Else if the chromosomes are to be further sub-divided.
    } else {
      my ($chrMin, $chrMax);
      my @tempArray = ();
      my $kiloBase   = 1000;

      if (defined $main::referenceSequence) {
        $tempArray[0] = $main::referenceSequence;
        $chrMin = 0;
        $chrMax = 1;
      } else {
        $chrMin = 0;
        $chrMax = 24;
        for (my $chr = 1; $chr < 23; $chr++) {$tempArray[$chr-1] = $chr;}
        $tempArray[22] = "X";
        $tempArray[23] = "Y";
      }

      for (my $chr = $chrMin; $chr < $chrMax; $chr++) {
        my $start  = 0;
        my $end    = $kiloBase*$main::targetRegionSize;
        my $extent = getChromosomeExtents($tempArray[$chr]);
        while() {
          push(@target_regions::targetRegions, "$tempArray[$chr]:$start-$end");
          $start += $kiloBase*$main::targetRegionSize;
          $end   += $kiloBase*$main::targetRegionSize;
          if ($end > $extent) {
            $end = $extent;
            push(@target_regions::targetRegions, "$tempArray[$chr]:$start-$end");
            last;
          }
        }
      }
    }

  # Variant call on the whole genome in one pass.
  } elsif ($main::divideGenome eq "w") {
    print STDERR ("WHOLE GENOME CALLING NOT YET IMPLEMENTED\n");
    exit(1);

  # Variant call on bed regions.
  } elsif ($main::divideGenome eq "b") {
    print STDERR ("BED FILE CALLING NOT YET IMPLEMENTED\n");
    exit(1);

  # Unknown options.
  } else {
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("Options -divide-genome can take one of the following values:\n");
    print STDERR ("\tw - whole genome,\n");
    print STDERR ("\tc - break up by chromosome (can only use -divide in conjunction with this),\n");
    print STDERR ("\tb - define regions with a bed file.\n\n");
    print STDERR ("Error in target_regions::defineRegions.\n");
    exit(1);
  }
}

# Use the reference dictionary file to get chromosome extents.
sub getChromosomeExtents {
  my $chromosome = $_[0];
  my $extent;

  open(DICT, "<$main::referenceBin/$main::referenceDictionary") ||
    die("Couldn't find reference dictionary file:\n\t$main::referenceBin/$main::referenceDictionary\n");
  while(<DICT>) {
    chomp;
    if ($_ !~ /^(\@SQ|\@HD)/) {
      print STDERR ("\n***SCRIPT TERMINATED***\n\n");
      print STDERR ("No information on chromosome $chromosome in file:\n\t$main::referenceBin/$main::referenceDictionary\n");
      exit(1);
    }
    if ($_ =~ /^\@SQ/) {
      my $dictionaryChromosome = (split(/:/, ( (split(/\t/, $_) )[1])))[1];
      if ($dictionaryChromosome =~ /^$chromosome$/) {
        $extent = (split(/\t/, $_))[2];
        $extent = (split(/:/, $extent))[1];
      }
    }
  }
  close(DICT);

  # If the defined chromosome does not extent in the reference
  # dictionsry, terminate with an error.
  if (!defined $extent) {
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("Reference sequence \"$chromosome\" does not exist in the reference dictionary:\n");
    print STDERR ("\t$main::referenceBin/$main::referenceDictionary\n");
    print STDERR ("Error in target_regions::getChromosomeExtents.\n");
    exit(1);
  }

  return $extent;
}

1;
