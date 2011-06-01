#!/usr/bin/perl -w

package mosaik;

use strict;
use POSIX;

# The alignment parameters used by Mosaik are determined by the
# sequencing technology and the read length.  This subroutine defines
# the different parameter sets that can be used.
sub mosaikParameters {

# The first set of parameters are as used for Mosaik version 1 in the
# 1000 genomes project, up to and including phase 1 calls.  The
# parameters used for Mosaik version 2 are the defaults of the
# aligner, but they are included here and in the Mosaik command line
# in order to ensure that it is clear to the user which parameters
# are being used.
#
# Set 0 -
#
# Default set for unknown read length
  $mosaik::parameters{0}{"-mmp"} = 0.05;
  $mosaik::parameters{0}{"-act"} = 20;
  $mosaik::parameters{0}{"-mhp"} = 100;
  $mosaik::parameters{0}{"-bw"}  = 13;
  $mosaik::parameters{0}{"-ls"}  = 100;

# Set 1 -
#
# Technology = illumina
# Readlength <= 43
  $mosaik::parameters{1}{"-mm"}  = 4;
  $mosaik::parameters{1}{"-mhp"} = 100;
  $mosaik::parameters{1}{"-act"} = 20;
  $mosaik::parameters{1}{"-bw"}  = 13;
  $mosaik::parameters{1}{"-ls"}  = 100;

# Set 2 -
#
# Technology = illumina
# 43 <= Readlength <= 63
  $mosaik::parameters{2}{"-mm"}  = 6;
  $mosaik::parameters{2}{"-mhp"} = 100;
  $mosaik::parameters{2}{"-act"} = 25;
  $mosaik::parameters{2}{"-bw"}  = 17;
  $mosaik::parameters{2}{"-ls"}  = 100;

# Set 3 -
#
# Technology = illumina
# Readlength > 63
  $mosaik::parameters{3}{"-mm"}  = 12;
  $mosaik::parameters{3}{"-mhp"} = 100;
  $mosaik::parameters{3}{"-act"} = 35;
  $mosaik::parameters{3}{"-bw"}  = 29;
  $mosaik::parameters{3}{"-ls"}  = 100;

# Set 4 -
#
# Technology = 454 / SOLiD
# Readlength <= 350
  $mosaik::parameters{4}{"-mmp"} = 0.05;
  $mosaik::parameters{4}{"-mhp"} = 200;
  $mosaik::parameters{4}{"-act"} = 26;
  $mosaik::parameters{4}{"-bw"}  = 51;

# Set 5 -
#
# Technology = 454 / SOLiD
# Readlength > 350
  $mosaik::parameters{5}{"-mmp"} = 0.05;
  $mosaik::parameters{5}{"-mhp"} = 200;
  $mosaik::parameters{5}{"-act"} = 55;
  $mosaik::parameters{5}{"-bw"}  = 51;

# Set 6 -
#
# The parameters used for Mosaik version 2.
  $mosaik::parameters{6}{"-mmp"} = 0.15;
  $mosaik::parameters{6}{"-mhp"} = 200;

  # The remaining parameters are dependent on the
  # input data and so are determined on the fly in
  # the aligner routine.
}

# Select the parameter set based on the technology and read length. 
sub parameterSetSelect {
  my $stdout     = $_[0];
  my $run        = $_[1];
  my $readLength = $_[2];

  my $technology = $main::sampleInfo{$stdout}->{TECHNOLOGY};
  my $parameterSet;

  # If using Mosaik version 2, use parameter set 6.
  if (defined $main::mosaikVersion2) {
    $parameterSet = 6;

  # Calculate the read length as basepair count / read count.  If either of
  # of these variables are not numberic, set the read length to zero and choose
  # the default parameter set (set 0).
  } else {
    if ($readLength = 0) {$parameterSet = 0;}
    else {
      if ($technology =~ /illumina/i) {
        if ($readLength <= 43) {$parameterSet = 1;}
        if ($readLength > 43 && $readLength <= 63) {$parameterSet = 2;}
        if ($readLength > 63) {$parameterSet = 3;}
      } elsif ($technology =~ /454/i || $technology =~ /solid/i) {
        if ($readLength <= 350) {$parameterSet = 4;}
        if ($readLength > 350) {$parameterSet = 5;}
      }
    }
  }

  return $parameterSet;
}

# Use MosaikBuild to convert the fastq input files into a Mosaik readable
# format.

sub mosaikBuild {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $run    = $_[3];
  my $build = "$main::runFileName.mkb";

# If MosaikBuild is to be run on the local disk, make sure that the build
# directory exists and set the BUILD_DIR to this location.
  print $script ("###\n### Use MosaikBuild to build input files for the Mosaik aligner\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE}, $main::task->{FILE2});
  general_tools::setOutputs($script, $stdout, $build);
  if (! defined $main::mosaikVersion2) {
    print $script ("  $main::modules{MOSAIKBUILDV1}->{BIN}/$main::modules{MOSAIKBUILDV1}->{COMMAND} \\\n");
  } else {
    print $script ("  $main::modules{MOSAIKBUILDV2}->{BIN}/$main::modules{MOSAIKBUILDV2}->{COMMAND} \\\n");
  }
  print $script ("  -q \$INPUT_DIR/\$INPUT \\\n");
  if (defined $main::task->{FILE2}) {print $script ("  -q2 \$INPUT_DIR/\$INPUT2 \\\n");}
  print $script ("  -mfl $main::runInfo{$run}->{FRAGMENT} \\\n");
  print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  -sam \"$main::sampleInfo{$stdout}->{SAMPLE}\" \\\n");
  print $script ("  -st \"$main::sampleInfo{$stdout}->{TECHNOLOGY}\" \\\n");
  print $script ("  -cn \"$main::runInfo{$run}->{CENTRE}\" \\\n");
  print $script ("  -id \"$run\" \\\n");
  print $script ("  -ln \"$main::runInfo{$run}->{LIBRARY}\" \\\n");
  print $script ("  -pu \"$main::runInfo{$run}->{PLATFORM}\" \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr \n\n");
  script_tools::fail(
    $script,
    "MosaikBuild",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::removeInput($script, $stdout, "\$INPUT");
  general_tools::updateTask($stdout, $build);
  general_tools::iterateTask($stdout, \@main::alignTasks);
}

# Align the reads to the reference using MosaikAligner.
sub mosaikAligner {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $run    = $_[3];
  my $align  = $main::runFileName;

  if (defined $main::mosaikVersion2) {
    print $script ("###\n### Use MosaikAligner (version 2) to align the input reads to the reference.\n###\n\n");
  } else {
    print $script ("###\n### Use MosaikAligner (version 1) to align the input reads to the reference.\n###\n\n");
    $align = "$align.mka";
  }
  general_tools::setInputs($script, $stdout, $main::task->{FILE}, "");
  general_tools::setOutputs($script, $stdout, $align);

# Ensure that the correct references are available in the expected location.
  print $script ("# Define required references and jump database.\n\n");
  if ($main::modules{$main::task->{TASK}}->{INPUT} eq "local") {
    print $script ("  REF_BIN=$main::referenceBin\n");
    print $script ("  REF=$main::mosaikRef\n");
    if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "solid") {
      print $script ("  COLORSPACE_REF=$main::mosaikCSRef\n\n");
      print $script ("  JUMP_BIN=$main::referenceBin\n");
      print $script ("  JUMP=$main::mosaikJumpCS\n\n");
    }
    else {
      print $script ("\n  JUMP_BIN=$main::referenceBin\n");
      print $script ("  JUMP=$main::mosaikJump\n\n");
    }
  } elsif ($main::modules{$main::task->{TASK}}->{INPUT} eq "node") {
    print $script ("  LOCAL_REF_BIN=$main::referenceBin\n");
    print $script ("  REF_BIN=$main::nodeBin\n");
    print $script ("  REF=$main::mosaikRef\n");
    if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "solid") {print $script ("  COLORSPACE_REF=$main::mosaikCSRef\n");}
    print $script ("  if [ ! -d \$REF_BIN ]; then mkdir -p \$REF_BIN; fi\n");
    print $script ("  if [ ! -f \$REF_BIN/\$REF ]; then TransferFiles \$LOCAL_REF_BIN \$REF_BIN \$REF; fi\n");
    if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "solid") {
      print $script ("  if [ ! -f \$REF_BIN/\$COLORSPACE_REF ]; then TransferFiles \$LOCAL_REF_BIN \$REF_BIN \$COLORSPACE_REF; fi\n");
    }
    print $script ("\n  LOCAL_JUMP_BIN=$main::referenceBin\n");
    print $script ("  JUMP_BIN=$main::nodeBin\n");
    if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "solid") {print $script ("  JUMP=$main::mosaikJumpCS\n");}
    else {print $script ("  JUMP=$main::mosaikJump\n");}
    print $script ("  if [ ! -d \$JUMP_BIN ]; then mkdir -p \$JUMP_BIN; fi\n");
    print $script ("  if [ ! -f \$JUMP_BIN/\$JUMP\\_keys.jmp ]; then TransferFiles \$LOCAL_JUMP_BIN \$JUMP_BIN \$JUMP\\_keys.jmp; fi\n");
    print $script ("  if [ ! -f \$JUMP_BIN/\$JUMP\\_meta.jmp ]; then TransferFiles \$LOCAL_JUMP_BIN \$JUMP_BIN \$JUMP\\_meta.jmp; fi\n");
    print $script ("  if [ ! -f \$JUMP_BIN/\$JUMP\\_positions.jmp ]; then TransferFiles \$LOCAL_JUMP_BIN \$JUMP_BIN \$JUMP\\_positions.jmp; fi\n\n");
  }

# Write out the command line.
  if (defined $main::mosaikVersion2) {
    print $script ("  $main::modules{MOSAIKALIGNERV2}->{BIN}/$main::modules{MOSAIKALIGNERV2}->{COMMAND} \\\n");
  } else {
    print $script ("  $main::modules{MOSAIKALIGNERV1}->{BIN}/$main::modules{MOSAIKALIGNERV1}->{COMMAND} \\\n");
  }
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");

# If using the low memory version of Mosaik, include the -lm tag.
  if (defined $main::lowMemory) {print $script ("  -lm \\\n");}
  if ($main::sampleInfo{$stdout}->{TECHNOLOGY} =~ /solid/i) {
    print $script ("  -ia \$REF_BIN/\$COLORSPACE_REF \\\n");
    print $script ("  -ibs ");
  } else {
    print $script ("  -ia ");
  }
  print $script ("\$REF_BIN/\$REF \\\n");

# Get the appropriate parameter set.
  my $readCount  = $main::runInfo{$run}->{READCOUNT};
  my $bpCount    = $main::runInfo{$run}->{BPCOUNT};
  my $readLength = ($readCount =~ /\D/ || $bpCount =~ /\D/) ? 0 : $bpCount / $readCount;
  my $parameterSet = parameterSetSelect($stdout, $run, $readLength);
  foreach my $key (keys %{$mosaik::parameters{$parameterSet}}) {
    if ($key eq "-ls") {
      if ($main::task->{READTYPE} =~ /paired/i) {
        print $script ("  $key $mosaik::parameters{$parameterSet}{$key} \\\n");
      }
    } else {
      print $script ("  $key $mosaik::parameters{$parameterSet}{$key} \\\n");
    }
  }

  # If using Mosaik version 2, include some extra commands.  Also
  # a number of the parameters are dependent on the particular
  # fragment length and read length, so are determined here.
  if (defined $main::mosaikVersion2) {
    my $act = 13 + ($readLength / 5);

    # Round bw up to the nearest integer.
    my $bw  = ceil(2.5 * $mosaik::parameters{$parameterSet}{"-mmp"} * $readLength);

    # Ensure that bw is odd.
    $bw = ( ($bw & 1) == 0) ? $bw + 1 : $bw;
    print $script ("  -sref moblist \\\n");
    print $script ("  -srefn 50 \\\n");
    print $script ("  -ls $main::runInfo{$run}->{FRAGMENT} \\\n");
    print $script ("  -act $act \\\n");
    print $script ("  -bw $bw \\\n");
  }
  print $script ("  -j \$JUMP_BIN/\$JUMP \\\n");
  if (!defined $main::threads) {$main::threads = 8;}
  print $script ("  -p $main::threads \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
  script_tools::fail(
    $script,
    "MosaikAligner",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::removeInput($script, $stdout, $main::task->{FILE});
  general_tools::updateTask($stdout, $align);
  general_tools::iterateTask($stdout, \@main::alignTasks);
}

# Sort the alignments using MosaikSort.
sub mosaikSort {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $run    = $_[3];
  my $sort   = "$main::runFileName.mks";

# Retrieve information about the run.
  print $script ("###\n### Use MosaikSort to sort the alignment archive.\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE},"");
  general_tools::setOutputs($script, $stdout, $sort);
  print $script ("  $main::modules{MOSAIKSORT}->{BIN}/$main::modules{MOSAIKSORT}->{COMMAND} \\\n");
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
  script_tools::fail(
    $script,
    "MosaikSort",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::removeInput($script, $stdout, $sort);
  general_tools::updateTask($stdout, $sort);
  general_tools::iterateTask($stdout, \@main::alignTasks);
}

# Convert the output into bam format.
sub mosaikText {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $run    = $_[3];
  my $bam    = "$main::runFileName.bam";

# Retrieve information about the run.
  print $script ("###\n### Use MosaikText to convert sorted Mosaik archive to bam format.\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE},"");
  general_tools::setOutputs($script, $stdout, $bam);
  print $script ("  $main::modules{MOSAIKTEXT}->{BIN}/$main::modules{MOSAIKTEXT}->{COMMAND} \\\n");
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  -bam \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  -u \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
  script_tools::fail(
    $script,
    "MosaikText",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::removeInput($script, $stdout, $bam);
  general_tools::updateTask($stdout, $bam);
  general_tools::iterateTask($stdout, \@main::alignTasks);
}

1;
