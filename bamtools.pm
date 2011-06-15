#!/usr/bin/perl -w

package bamtools;

use strict;

# Use bamtools to sort a bam file.
sub sortMosaikv2Bam {
  my $script = $_[0];
  my $stdout = $_[1];
  my $retain = $_[2];
  my $dir    = $_[3];
  my $sort   = "$main::task->{FILE}.sorted";

  print $script ("###\n### Use bamtools to sort the bam files.\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE});
  general_tools::setOutputs($script, $stdout, $sort);

# Sort the regular bam file.  Check that there are alignments in the
# bam file first.  Only sort the file if the file has finite size.
# If there are no 'special' reference sequence in the reference, the
# special bam file will have zero size.  An attempt to sort a file of
# zero size will result in an exception and thus terminate the pipeline.
  print $script ("# Main bam file.\n\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT.bam ]; then\n");
  print $script ("    $main::modules{BAMTOOLS}->{BIN}/$main::modules{BAMTOOLS}->{COMMAND} sort \\\n");
  print $script ("    -n 5000000 \\\n");
  print $script ("    -in \$INPUT_DIR/\$INPUT.bam \\\n");
  print $script ("    -out \$OUTPUT_DIR/\$OUTPUT.bam \\\n");
  print $script ("    > \$OUTPUT_DIR/\$OUTPUT.bam.stdout \\\n");
  print $script ("    2> \$OUTPUT_DIR/\$OUTPUT.bam.stderr\n");
  print $script ("  fi\n\n");
  script_tools::fail(
    $script,
    "bamtools sort (regular bam)",
    "\$INPUT.bam",
    "\$OUTPUT.bam.stdout",
    "\$OUTPUT.bam.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT.bam ]; then\n");
  print $script ("    mv \$OUTPUT_DIR/\$OUTPUT.bam \$OUTPUT_DIR/\$INPUT.bam\n");
  print $script ("  fi\n\n");

# Sort the multiply mapped bam file.
  print $script ("# Multiply mapped bam file.\n\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT.multiple.bam ]; then\n");
  print $script ("    $main::modules{BAMTOOLS}->{BIN}/$main::modules{BAMTOOLS}->{COMMAND} sort \\\n");
  print $script ("    -n 5000000 \\\n");
  print $script ("    -in \$INPUT_DIR/\$INPUT.multiple.bam \\\n");
  print $script ("    -out \$OUTPUT_DIR/\$OUTPUT.multiple.bam \\\n");
  print $script ("    > \$OUTPUT_DIR/\$OUTPUT.multiple.bam.stdout \\\n");
  print $script ("    2> \$OUTPUT_DIR/\$OUTPUT.multiple.bam.stderr\n");
  print $script ("  fi\n\n");
  script_tools::fail(
    $script,
    "bamtools sort (multiple bam)",
    "\$INPUT.multiple.bam",
    "\$OUTPUT.multiple.bam.stdout",
    "\$OUTPUT.multiple.bam.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT.multiple.bam ]; then\n");
  print $script ("    mv \$OUTPUT_DIR/\$OUTPUT.multiple.bam \$OUTPUT_DIR/\$INPUT.multiple.bam\n");
  print $script ("  fi\n\n");

# Sort the special bam file.
  print $script ("# Special reference sequences bam file.\n\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT.special.bam ]; then\n");
  print $script ("    $main::modules{BAMTOOLS}->{BIN}/$main::modules{BAMTOOLS}->{COMMAND} sort \\\n");
  print $script ("    -n 5000000 \\\n");
  print $script ("    -in \$INPUT_DIR/\$INPUT.special.bam \\\n");
  print $script ("    -out \$OUTPUT_DIR/\$OUTPUT.special.bam \\\n");
  print $script ("    > \$OUTPUT_DIR/\$OUTPUT.special.bam.stdout \\\n");
  print $script ("    2> \$OUTPUT_DIR/\$OUTPUT.special.bam.stderr\n");
  print $script ("  fi\n\n");
  script_tools::fail(
    $script,
    "bamtools sort (special bam)",
    "\$INPUT.special.bam",
    "\$OUTPUT.special.bam.stdout",
    "\$OUTPUT.special.bam.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT.special.bam ]; then\n");
  print $script ("    mv \$OUTPUT_DIR/\$OUTPUT.special.bam \$OUTPUT_DIR/\$INPUT.special.bam\n");
  print $script ("  fi\n\n");

  my $sample = $main::sampleInfo{$stdout}->{SAMPLE};
  if ($retain eq "yes" && $main::task->{LOCATION} eq "node") {
    $main::retainFiles{"$main::task->{FILE}.bam"}           = "$main::outputDirectory/$main::aligner/$sample/$dir";
    $main::retainFiles{"$main::task->{FILE}.multiple.bam"}  = "$main::outputDirectory/$main::aligner/$sample/$dir";
    $main::retainFiles{"$main::task->{FILE}.unaligned.bam"} = "$main::outputDirectory/$main::aligner/$sample/$dir";
    $main::retainFiles{"$main::task->{FILE}.special.bam"}   = "$main::outputDirectory/$main::aligner/$sample/$dir";
  } elsif ($retain eq "no" && $main::task->{LOCATION} eq "local") {
    $main::deleteFiles{"$main::task->{FILE}.bam"}           = "$main::outputDirectory/$main::aligner/$sample/$dir";
    $main::deleteFiles{"$main::task->{FILE}.multiple.bam"}  = "$main::outputDirectory/$main::aligner/$sample/$dir";
    $main::deleteFiles{"$main::task->{FILE}.unaligned.bam"} = "$main::outputDirectory/$main::aligner/$sample/$dir";
    $main::deleteFiles{"$main::task->{FILE}.special.bam"}   = "$main::outputDirectory/$main::aligner/$sample/$dir";
  }
  general_tools::updateTask($stdout, "$main::task->{FILE}.bam");
  general_tools::iterateTask($stdout, \@main::alignTasks);
}

# Use bamtools to index the bam file.

sub index {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $run    = $_[3];

  print $script ("###\n### Use bamtools to index the bam file.  It is necessary\n");
  print $script ("### to be in the same physical directory as the bam file for this step\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE});
  print $script ("  cd \$INPUT_DIR\n\n");
  print $script ("  $main::modules{BAMTOOLS}->{BIN}/$main::modules{BAMTOOLS}->{COMMAND} index \\\n");
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  > \$INPUT_DIR/\$INPUT.bai.stdout \\\n");
  print $script ("  2> \$INPUT_DIR/\$INPUT.bai.stderr\n\n");
  script_tools::fail(
    $script,
    "bamtools index",
    "\$INPUT",
    "\$INPUT.bai.stdout",
    "\$INPUT.bai.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::iterateTask($stdout, \@tasks);
}

# Get the read groups from a bam file.
sub GetReadGroups {
  my $bam=$_[0];
  my %RG=();

  my $command = "$main::modules{BAMTOOLS}->{BIN}/$main::modules{BAMTOOLS}->{COMMAND} header";
  my @header = `$command -in $bam`;
  foreach my $line (@header) {
    chomp($line);
    if ($line =~ /^\@RG/) {$RG{(split(/:/, (split(/\t/, $line))[1]))[-1]} = 1;}
  }

  return %RG;
}

# Remove read groups from a bam file by creating a json filter script and
# using the bamtools filter functionality.  Also strip out the duplicate
# marks.
sub modifyBam {
  my $script   = $_[0];
  my $stdout   = $_[1];
  my @tasks    = @{$_[2]};
  my @removeRG = ();
  my $bam;

  if ($main::sampleInfo{$stdout}->{FILE} ne "") {
    $bam = "$main::sampleInfo{$stdout}->{PATH}/$main::sampleInfo{$stdout}->{FILE}";
  } else {
    $bam = "";
  }

# This task is only performed if there exists a merged bam file and that there
# are runs that require withdrawal.
  if ($main::sampleInfo{$stdout}->{STATUS} eq "withdraw") {

# A new bam file will be created from the existing one.  If the new file has the
# same name as the old one, then an error has occurred.
    if ($main::sampleInfo{$stdout}->{FILE} eq "$main::mergeFileName.bam") {
      print("\n***SCRIPT TERMINATED***\n\n");
      print("Trying to create a new bam file that has the same name as an existing one.\n");
      print("$main::sampleInfo{$stdout}->{FILE}\n");
      die("Error in bamtools::modifyBam.\n");
    }

    general_tools::setInputs($script, $stdout, $main::task->{FILE});
    general_tools::setOutputs($script, $stdout, $bam);
    print $script ("###\n### Use bamtools to remove unwanted read groups from the bam file.\n###\n\n");
    foreach my $run (keys %{$main::sampleInfo{$stdout}->{RUN}}) {
      if ($main::runInfo{$run}->{STATUS} eq "withdrawn") {push(@removeRG, $run);}
      if ($main::runInfo{$run}->{STATUS} eq "realign") {push(@removeRG, $run);}
    }
    if (scalar @removeRG == 0 ) {
      print("\n***SCRIPT TERMINATED***\n\n");
      print("Trying to remove read groups, but none found.\n");
      print("$stdout\n");
      die("Error in bamtools::modifyBam.\n");
    }

    my $filterName = $main::mergeFileName;
    filter($script, $stdout, $filterName, \@removeRG);
    removeDuplicateFlags($script, $stdout, \@tasks);

# Update the status of the existing bam file.  If ALIGN equals yes, then there are
# some more read groups to add to the bam file.  In this case, set the status to add.
# If ALIGN equals no, then there are no more read groups to add and the status can be
# set to process (duplicate marking still needs to be performed).
    if ($main::sampleInfo{$stdout}->{ALIGN} eq "yes") {$main::sampleInfo{$stdout}->{STATUS}="add";}
    else {$main::sampleInfo{$stdout}->{STATUS}="process";}
    general_tools::iterateTask($stdout, \@tasks);

# If the status of the bam file is "add", strip out all duplicate marks.
  } elsif ($main::sampleInfo{$stdout}->{STATUS} eq "add") {
    removeDuplicateFlags($script, $stdout, \@tasks);
    general_tools::iterateTask($stdout, \@tasks);

# If the status is "complete", set the TASK to complete and nothing further is required.
  } elsif ($main::sampleInfo{$stdout}->{STATUS} eq "complete") {
    $main::task->{TASK}="Complete";

# If status is none of the above, a file will be created.
  } else {
    general_tools::iterateTask($stdout, \@tasks);
  }
}

# Create a Json script for filtering.
sub filter {
  my $script  = $_[0];
  my $stdout  = $_[1];
  my $json    = $_[2];
  my @filters = @{$_[3]};
  my $number  = 1;
  my $rule    = "";

  print $script ("###\n### Create a json script listing the filtering rules.\n###\n\n");
  print $script ("  JSON_FILTER=$json.json\n");
  print $script ("  INPUT=$main::task->{FILE}\n");
  print $script ("  OUTPUT=$main::mergeFileName.temp.bam\n\n");
  print $script ("  echo \"{\" > \$NODE_DIR/\$JSON_FILTER\n");
  print $script ("  echo \"  \\\"filters\\\" : [ \" >> \$NODE_DIR/\$JSON_FILTER\n");
  foreach my $filter (@filters) {
    print $script ("  echo \"                  {\" >> \$NODE_DIR/\$JSON_FILTER\n");
    print $script ("  echo \"                    \\\"id\\\" : \\\"filter$number\\\",\" >> \$NODE_DIR/\$JSON_FILTER\n");
    print $script ("  echo \"                    \\\"tag\\\" : \\\"RG:$filter\\\"\" >> \$NODE_DIR/\$JSON_FILTER\n");
    if ($number != scalar @filters) {print $script ("  echo \"                  },\" >> \$NODE_DIR/\$JSON_FILTER\n");}
    else {print $script ("  echo \"                  }\" >> \$NODE_DIR/\$JSON_FILTER\n");}
    if ($number == 1) {$rule = "filter$number";}
    else {$rule = "$rule | filter$number";}
    $number++;
  }
  print $script ("  echo \"                ],\" >> \$NODE_DIR/\$JSON_FILTER\n");
  print $script ("  echo \"  \\\"rule\\\" : \\\"!($rule)\\\"\" >> \$NODE_DIR/\$JSON_FILTER\n");
  print $script ("  echo \"}\" >> \$NODE_DIR/\$JSON_FILTER\n\n");
  print $script ("###\n### Use bamtools to filter the bam file.\n###\n\n");
  print $script ("  $main::modules{$main::task->{TASK}}->{BIN}/$main::module{$main::task->{TASK}}->{COMMAND} filter");
  print $script ("  -script \$NODE_DIR/\$JSON_FILTER \\\n");
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
  script_tools::fail(
    $script,
    "bamtools filter",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  $main::task->{FILE}="$main::mergeFileName.temp.bam";
}

# Remove the duplicate mark flags from a bam file.
sub removeDuplicateFlags {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $task   = $main::task->{TASK};

  print $script ("###\n### Remove duplicate flags from bam file.\n###\n\n");
  $main::task->{TASK}="REMOVE_DUPLICATE_FLAGS";
  general_tools::setInputs($script, $stdout, $main::task->{FILE});
  #general_tools::setOutputs($script, $stdout, $bam);
  print $script ("  INPUT=$main::task->{FILE}\n");
  print $script ("  OUTPUT=$main::mergeFileName.bam\n\n");
  print $script ("  $main::modules{$main::task->{TASK}}->{BIN}/$main::ModuleInfo{$main::Task->{TASK}}->{COMMAND} filter \\\n");
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  -keepQualities \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
  script_tools::fail(
    $script,
    "bamtools revert (remove duplicate flags)",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::iterateTask($stdout, \@main::alignTasks);
  $main::task->{TASK} = $task;
}

# Merge a set of bam files.
sub mergeBamFiles {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $output = "$main::mergeFileName.merged.bam";

# Count the number of files to merge.
  my $numberBams = 0;
  if ($main::task->{FILE} ne "") {$numberBams++;}
  foreach my $run (@{$main::sampleInfo{$stdout}->{ALIGNED_BAM}}) {$numberBams++;}

  if ($main::sampleInfo{$stdout}->{STATUS} ne "process" && $numberBams > 0) {
    print $script ("###\n### Use bamtools to merge together bam files for:\n");
    print $script ("### Sample: $main::sampleInfo{$stdout}->{SAMPLE}\n");
    print $script ("### Technology: $main::sampleInfo{$stdout}->{TECHNOLOGY}\n###\n\n");
    general_tools::setOutputs($script, $stdout, $output);
    if ($main::task->{FILE} ne "") {print $script ("  INPUT=$main::task->{FILE}\n");}
    print $script ("  $main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} merge \\\n");
    if ($main::task->{FILE} ne "") {print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");}
    foreach my $run (@{$main::sampleInfo{$stdout}->{ALIGNED_BAM}}) {
      my @fileTags = split(/\./,$run);
      my $readType = $fileTags[-3];
      my $runid    = $fileTags[1];
      my $status   = "run";
      if ($readType eq "SINGLE" && exists $main::runInfo{$runid}->{SE_STATUS}) {$status = $main::runInfo{$runid}->{SE_STATUS};}
      elsif ($readType eq "PAIRED" && exists $main::runInfo{$runid}->{PE_STATUS}) {$status = $main::runInfo{$runid}->{PE_STATUS};}
      if ($status eq "run" || ! defined $main::noFailed) {print $script ("  -in $run \\\n");}
    }
    print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");
    print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
    print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
    script_tools::fail(
    $script,
      "bamtools merge",
      "\$OUTPUT",
      "\$OUTPUT.stdout",
      "\$OUTPUT.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );
    general_tools::updateTask($stdout, $output);
    general_tools::iterateTask($stdout, \@tasks);
  } else {
    general_tools::iterateTask($stdout, \@tasks);
  }
}

# Generate statistics on a bam file.
sub statistics {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};

  (my $stats = $main::task->{FILE}) =~ s/bam$/bam.bts/;

  print $script ("###\n### Generate statistics on the bam file.\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE}, $main::task->{FILE2});
  general_tools::setOutputs($script, $stdout, $stats);
  print $script ("  $main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} stats \\\n");
  print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
  script_tools::fail(
    $script,
    "bamtools stats",
    "",
    "\$OUTPUT",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::iterateTask($stdout, \@tasks);
}

# Use bamtools resolve to generate the fragment length statistics.
sub resolve {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};

  (my $stat = $main::task->{FILE}) =~ s/bam$/bstat/;
  if ($stat =~ /\.recal/) {$stat =~ s/\.recal//;}
  (my $bam  = $main::task->{FILE}) =~ s/bam$/paired\.bam/;

  if ($main::task->{READTYPE} eq "PAIRED") {
    print $script ("###\n### Generate bstats file using bamtools resolve.\n###\n\n");
    general_tools::setInputs($script, $stdout, $main::task->{FILE});
    general_tools::setOutputs($script, $stdout, $bam);
    print $script ("  $main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} resolve \\\n");
    print $script ("  -in \$INPUT_DIR/\$INPUT \\\n");
    print $script ("  -twoPass \\\n");
    print $script ("  -stats \$OUTPUT_DIR/$stat \\\n");
    print $script ("  -out \$OUTPUT_DIR/\$OUTPUT \\\n");
    print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
    print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
    script_tools::fail(
      $script,
      "bamtools resolve",
      "",
      "\$OUTPUT",
      "\$OUTPUT.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );
    general_tools::removeInput($script, $stdout, "\$INPUT");
    general_tools::updateTask($stdout, $bam);

# Add the stats file to an array so that all of these can be merged.
    push(@main::bamtoolsStats, "$main::sampleInfo{$stdout}->{SAMPLE}/merged/$stat");
  }
  general_tools::iterateTask($stdout, \@tasks);
}

1;
