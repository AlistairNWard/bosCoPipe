#!/usr/bin/perl -w

use strict;
use File::Find;

sub fileSearch {
  my $extension;
  my @extensions;
  chomp;

# If this is part of an incrementsl run, check if this is a bam file from a previous release.
  if ($_ =~ /\.bam$/ && $_ =~ /$main::aligner/ && $_ !~ /single/i && $_ !~ /paired/i) {
    if (/$main::date/) {
      push(@main::currentIncrementBams, "$File::Find::dir/$_");
    } elsif (defined $main::previousDate && $_ =~ /$main::previousDate/) {
      push(@main::previousIncrementBams, "$File::Find::dir/$_");
    } else {
      push(@main::previousBams, "$File::Find::dir/$_");
    }
  } elsif ($_ =~ /fastq/i) {
    $main::fastq{$_} = $File::Find::dir;
  } elsif ($_ =~ /\.glf/) {
    $main::existingGlf{$_} = $File::Find::dir;
  } elsif ($_ =~ /$main::date/ && $_ =~ /$main::aligner/) {
    push(@main::existingFiles, "$File::Find::dir/$_");
  }
}

# Search for fastq files only.
sub findFastq {
  chomp;
  if ($_ =~ /fastq/i) {
    if (exists $main::fastq{$_}) {
      print("\n\n***SCRIPT TERMINATED***\n\n");
      print("Fastq file \"$_\" appears twice:\n\t$main::fastq{$_}/$_\n\t$File::Find::dir/$_\n");
      die("Error in search::findFastq,\n");
    }
    $main::fastq{$_} = $File::Find::dir;
  }
}

1;
