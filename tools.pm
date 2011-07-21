#!/usr/bin/perl -w

package tools;

use strict;

# Use the fastQValidator tool to check the contents of the fastq
# file(s) if the pipeline entry is fastq.
sub fastQValidator {
  my $script     = $_[0];
  my $stdout     = $_[1];
  my $run        = $_[2];
  my $fastq1     = $main::task->{FILE};
  my $location   = $main::task->{LOCATION};
  my $path       = $main::task->{PATH};
  my $location   = $main::task->{LOCATION};

  my $iterations = ($main::task->{READTYPE} eq "PAIRED") ? 2 : 1;

  print $script ("###\n### Use fastQValidator to check the fastq files\n###\n\n");
  for (my $i = 0; $i < $iterations; $i++) {

# NewTask changed some of the contents of main::Task.  Reset these for
# the second bam file (paired end reads).
    if ($i == 1) {
      $main::task->{FILE}     = $main::task->{FILE2};
      $main::task->{LOCATION} = $location;
    }
    general_tools::setInputs($script, $stdout, $main::task->{FILE},"");
    general_tools::setOutputs($script, $stdout, "");

# Define the directory to run in.
    print $script ("  $main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} \\\n");
    print $script ("  --file \$INPUT_DIR/\$INPUT \\\n");
    print $script ("  > \$OUTPUT_DIR/\$INPUT.stdout \\\n");
    print $script ("  2> \$OUTPUT_DIR/\$INPUT.stderr\n\n");
    script_tools::fail(
      $script,
      "fastQValidator",
      "none",
      "\$INPUT.stdout",
      "\$INPUT.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );
    if ($i == 1) {$main::task->{FILE}=$fastq1;}
  }
  general_tools::iterateTask($stdout, \@main::alignTasks);
}

sub baseQualityRecalibration {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $memory;

  if (defined $main::BQRecal) {
    print $script ("###\n### Perform base quality recalibration using GATK.\n###\n\n");
    general_tools::setInputs($script, $stdout, $main::task->{FILE},"");
    general_tools::setOutputs($script, $stdout,"");
    print $script ("# Define files required by GATK.\n\n");
    print $script ("  REF=$main::reference\n");
    if ($main::modules{$main::task->{TASK}}->{INPUT} eq "local") {
      print $script ("  REF_BIN=$main::referenceBin\n");
    }
    elsif ($main::modules{$main::task->{TASK}}->{INPUT} eq "node") {
      print $script ("  REF_DICT=$main::referenceDictionary\n");
      print $script ("  REF_LOCAL=$main::referenceBin\n");
      print $script ("  REF_BIN=$main::nodeBin\n");
      print $script ("  if [ ! -d \$REF_BIN ]; then mkdir -p \$REF_BIN; fi\n");
      print $script ("  if [ ! -f \$REF_BIN/\$REF ]; then rsync \$REF_LOCAL/\$REF \$REF_BIN; fi\n");
      print $script ("  if [ ! -f \$REF_BIN/\$REF.fai ]; then rsync \$REF_LOCAL/\$REF.fai \$REF_BIN; fi\n");
      print $script ("  if [ ! -f \$REF_BIN/\$REF_DICT ]; then rsync \$REF_LOCAL/\$REF_DICT \$REF_BIN; fi\n");
    }

    print $script ("\n  DBSNP=$main::dbsnp\n");
    if ($main::modules{$main::task->{TASK}}->{INPUT} eq "local") {
      print $script ("  DBSNP_BIN=$main::dbsnpBin\n");
    } elsif ($main::modules{$main::task->{TASK}}->{INPUT} eq "node") {
      print $script ("  DBSNP_LOCAL=$main::dbsnpBin\n");
      print $script ("  DBSNP_BIN=$main::nodeBin\n");
      print $script ("  if [ ! -d \$DBSNP_BIN ]; then mkdir -p \$DBSNP_BIN; fi\n");
      print $script ("  if [ ! -f \$DBSNP_BIN/\$DBSNP ]; then rsync \$DBSNP_LOCAL/\$DBSNP \$DBSNP_BIN; fi\n");
    }

    print $script ("\n  INPUT=$main::task->{FILE}\n");
    print $script ("  OUTPUT=$main::runFileName.recal.bam\n");
    print $script ("  CSV=$main::runFileName.csv\n\n");

# The first step counts the covariates and generates the csv file.
    print $script ("# Count covariates\n\n");
    if (defined $main::nodeMemory) {($memory = $main::nodeMemory) =~ s/gb/g/g;}
    elsif (defined $main::lowMemory) {$memory = "8g";}
    else {$memory = "32g";}
    print $script ("  java -Xmx$memory -jar ");
    print $script ("$main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} \\\n");
    print $script ("  -R \$REF_BIN/\$REF \\\n");
    #print $script ("  --DBSNP \$DBSNP_BIN/\$DBSNP \\\n");
    print $script ("  -B:dbsnp,vcf \$DBSNP_BIN/\$DBSNP \\\n");
    print $script ("  -l INFO \\\n");
    print $script ("  -T CountCovariates \\\n");
    print $script ("  -I \$INPUT_DIR/\$INPUT \\\n");
    print $script ("  -cov ReadGroupCovariate \\\n");
    print $script ("  -cov QualityScoreCovariate \\\n");
    print $script ("  -cov CycleCovariate \\\n");
    print $script ("  -cov DinucCovariate \\\n");
    if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "solid") {
      print $script ("  --solid_nocall_strategy PURGE_READ \\\n");
      print $script ("  --solid_recal_mode SET_Q_ZERO \\\n");
      print $script ("  -dP solid \\\n");
    } elsif ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "454") {
      print $script ("  -dP 454 \\\n");
    } elsif ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "illumina") {
      print $script ("  -dP illumina \\\n");
    }
    print $script ("  -recalFile \$OUTPUT_DIR/\$CSV \\\n");
    print $script ("  -nt 8 \\\n");
    print $script ("  > \$OUTPUT_DIR/\$CSV.stdout \\\n");
    print $script ("  2> \$OUTPUT_DIR/\$CSV.stderr \n\n");
    script_tools::fail(
      $script,
      "GATK count covariates",
      "\$CSV",
      "\$CSV.stdout",
      "\$CSV.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );

# Now use the csv file to generate a new bam file with the recalibrated base
# qualities.  The original qualities are retained in the bam file so no information
# is lost and the original merged bam file can be deleted.
    print $script ("# Recalibrate\n\n");
    print $script ("  java -Xmx$memory -jar ");
    print $script ("$main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} \\\n");
    print $script ("  -R \$REF_BIN/\$REF \\\n");
    print $script ("  -T TableRecalibration \\\n");
    print $script ("  -I \$INPUT_DIR/\$INPUT \\\n");
    print $script ("  --out \$OUTPUT_DIR/\$OUTPUT \\\n");
    print $script ("  -recalFile \$OUTPUT_DIR/\$CSV \\\n");
    print $script ("  --doNotWriteOriginalQuals \\\n");
    print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
    print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
    script_tools::fail(
      $script,
      "GATK recalibration",
      "\$OUTPUT",
      "\$OUTPUT.stdout",
      "\$OUTPUT.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );
    general_tools::removeInput($script, $stdout, $main::task->{FILE});
    general_tools::updateTask($stdout, "$main::runFileName.recal.bam");
  }
  general_tools::iterateTask($stdout, \@tasks);
}

# Rename the final bam file.
sub renameBam {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};

  print $script ("###\n### Rename the final bam file.\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE}, "");
  general_tools::setOutputs($script, $stdout,  $main::renameFile);
  print $script ("  TransferFiles \$INPUT_DIR \$OUTPUT_DIR \$INPUT \$OUTPUT\n\n");
  general_tools::updateTask($stdout, $main::renameFile);
  general_tools::iterateTask($stdout, \@tasks);
}

# Having been sorted, the extra bam files from Mosaik2 need to be moved to the
# correct directory on the local disk.
sub moveMosaik2Bam {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};

  print $script ("###\n### Move the extra Mosaik2 bam files to the merged directory\n###\n\n");

  # Multiply mapped bam.
  (my $inputFile = $main::task->{FILE}) =~ s/$main::date/$main::date\.multiple/;
  if ($inputFile =~ /recal/) {$inputFile =~ s/.recal//g;}
  print $script ("  # Multiply mapped bam\n");
  print $script ("  INPUT=$inputFile\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT ]; then");
  print $script (" TransferFiles \$INPUT_DIR \$OUTPUT_DIR \$INPUT;");
  print $script (" rm -f \$INPUT_DIR/\$INPUT;");
  print $script (" fi\n\n");

  # Unaligned bam.
  (my $inputFile = $main::task->{FILE}) =~ s/$main::date/$main::date\.unaligned/;
  if ($inputFile =~ /recal/) {$inputFile =~ s/.recal//g;}
  print $script ("  # Unaligned bam\n");
  print $script ("  INPUT=$inputFile\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT ]; then");
  print $script (" TransferFiles \$INPUT_DIR \$OUTPUT_DIR \$INPUT;");
  print $script (" rm -f \$INPUT_DIR/\$INPUT;");
  print $script (" fi\n\n");

  # Special bam.
  (my $inputFile = $main::task->{FILE}) =~ s/$main::date/$main::date\.special/;
  if ($inputFile =~ /recal/) {$inputFile =~ s/.recal//g;}
  print $script ("  # Special bam\n");
  print $script ("  INPUT=$inputFile\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT ]; then");
  print $script (" TransferFiles \$INPUT_DIR \$OUTPUT_DIR \$INPUT;");
  print $script (" rm -f \$INPUT_DIR/\$INPUT;");
  print $script (" fi\n\n");

  # Stats files.
  (my $inputFile = $main::task->{FILE}) =~ s/$main::date/$main::date\.stat/;
  if ($inputFile =~ /\.bam/) {$inputFile =~ s/.bam//g;}
  print $script ("  # Stats file(s)\n");
  print $script ("  INPUT=$inputFile\n");
  print $script ("  if [ -s \$INPUT_DIR/\$INPUT ]; then");
  print $script (" TransferFiles \$INPUT_DIR \$OUTPUT_DIR \$INPUT;");
  print $script (" rm -f \$INPUT_DIR/\$INPUT;");
  print $script (" fi\n\n");
  if ($main::task->{READTYPE} eq "PAIRED") {
    $inputFile =~ s/stat$/bstat/;
    print $script ("  INPUT=$inputFile\n");
    print $script ("  if [ -s \$INPUT_DIR/\$INPUT ]; then");
    print $script (" TransferFiles \$INPUT_DIR \$OUTPUT_DIR \$INPUT;");
    print $script (" rm -f \$INPUT_DIR/\$INPUT;");
    print $script (" fi\n\n");
  }
  general_tools::iterateTask($stdout, \@tasks);
}

# Mark duplicate reads in the bam file using Picard.
sub duplicateMarkPicard {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $dupBam = "$main::mergeFileName.dupmarked.bam";

  if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "illumina" || $main::sampleInfo{$stdout}->{TECHNOLOGY} eq "solid") {
    print $script ("###\n### Mark duplicate reads using Picard\n###\n\n");
    general_tools::setInputs($script, $stdout, $main::task->{FILE},"");
    general_tools::setOutputs($script, $stdout,  $dupBam);
    print $script ("  METRICS=$main::mergeFileName.metrics\n\n");
    print $script ("  $main::modules{$main::task->{TASK}}->{PRE_COMMAND} ");
    print $script ("$main::modules{$main::task->{TASK}}->{BIN}/$main::modules{$main::task->{TASK}}->{COMMAND} \\\n");
    print $script ("  I=\$INPUT_DIR/\$INPUT \\\n");
    print $script ("  O=\$OUTPUT_DIR/\$OUTPUT \\\n");
    print $script ("  M=\$OUTPUT_DIR/\$METRICS \\\n");
    print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
    print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
    script_tools::fail(
      $script,
      "Picard duplicate marking",
      "\$OUTPUT",
      "\$OUTPUT.stdout",
      "\$OUTPUT.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );
    general_tools::removeInput($script, $stdout, $main::task->{FILE});
    general_tools::updateTask($stdout, $dupBam);
    general_tools::iterateTask($stdout, \@tasks);
  }
  else {
    general_tools::iterateTask($stdout, \@tasks);
  }
}

# Mark duplicate reads in the bam file using BCMMarkDupes.
sub duplicateMarkBCM {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};
  my $dupBam = "$main::mergeFileName.dupmarked.bam";

  if ($main::sampleInfo{$stdout}->{TECHNOLOGY} eq "454") {
    print $script ("###\n### Mark duplicate reads using BCMMarkDupes\n###\n\n");
    general_tools::setInputs($script, $stdout, $main::task->{FILE}, "");
    general_tools::setOutputs($script, $stdout,  $dupBam);
    print $script ("  BCM_BIN=$main::modules{$main::Task->{TASK}}->{BIN}\n\n");
    print $script ("  $main::modules{$main::task->{TASK}}->{PRE_COMMAND} \$BCM_BIN:\$BCM_BIM/");
    print $script ("$main::modules{$main::task->{TASK}}->{COMMAND} BCMMarkDupes \\\n");
    print $script ("  \$INPUT_DIR/\$INPUT \\\n");
    print $script ("  \$INPUT_DIR/\$INPUT.bai \\\n");
    print $script ("  \$OUTPUT_DIR/\$OUTPUT \\\n");
    print $script ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
    print $script ("2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");
    script_tools::fail(
      $script,
      "BCM duplicate marking",
      "\$OUTPUT",
      "\$OUTPUT.stdout",
      "\$OUTPUT.stderr",
      "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
    );
    general_tools::removeInput($script, $stdout, $main::task->{FILE});
    general_tools::updateTask($stdout, $dupBam);
    general_tools::iterateTask($stdout, \@tasks);
  } else {
    general_tools::iterateTask($stdout, \@tasks);
  }
}

# Calculate the md5sum of a file.
sub calculateMd5 {
  my $script = $_[0];
  my $stdout = $_[1];
  my @tasks  = @{$_[2]};

  print $script ("###\n### Calculate the md5 checksum for the merged bam file.\n###\n\n");
  general_tools::setInputs($script, $stdout, $main::task->{FILE}, "");
  general_tools::setOutputs($script, $stdout,  "$main::task->{FILE}.md5sum");
  print $script ("  $main::modules{$main::task->{TASK}}->{COMMAND} \$INPUT_DIR/\$INPUT \\\n");
  print $script ("  > \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $script ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr \n\n");
  script_tools::fail(
    $script,
    "md5 checksum",
    "\$OUTPUT",
    "",
    "\$OUTPUT.stderr",
    "$main::aligner/$main::sampleInfo{$stdout}->{SAMPLE}/failed"
  );
  general_tools::updateTask($stdout, "$main::task->{FILE}.md5sum");
  general_tools::iterateTask($stdout, \@tasks);
}

1;
