#!/usr/bin/perl -w

package pair_statistics;

use strict;

# Merge together stat files from different read groups to 
# generate a single file readable by bamtools.
sub mergeStats {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
}

1;
