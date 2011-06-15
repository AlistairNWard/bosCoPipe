#!/usr/bin/perl -w

package freebayes;

use strict;

# Use freebayes to perform SNP calling.

sub freebayes {

  # Define the region.
  $freebayes::region=$target_regions::targetRegions[$main::task->{REGION}];

  # Generate the file name for the files created in SNP calling.
  if ($main::aligner ne "none") {
    $main::snpFileName=join(".", 
      $main::snpCaller,
      $main::aligner,
      $freebayes::region,
    );
  } else {
    $main::snpFileName=join(".", 
      $main::snpCaller,
      $freebayes::region,
    );
  }

  $main::snpFileName = join(".", $main::snpFileName, $main::date);
  $freebayes::region =~ s/-/../;
  $freebayes::refSequence = (split(/:/, $freebayes::region))[0];

  if (!defined $main::queue) {$main::queue = "stage";}
  $freebayes::SCRIPT = script_tools::createScript($main::snpFileName, "SNP calling", 1, $main::queue);
  script_tools::scriptFail($freebayes::SCRIPT, $main::snpFileName);
  script_tools::transferFiles($freebayes::SCRIPT);

  print $freebayes::SCRIPT ("# Perform SNP calling with freeBayes\n\n");
  general_tools::setOutputs($freebayes::SCRIPT, $main::snpFileName, "$main::snpFileName.vcf");

  # Write out reference file information.
  print $freebayes::SCRIPT ("# Check for the reference files.\n\n");
  if ($main::modules{$main::task->{TASK}}->{INPUT} eq "node") {
    print $freebayes::SCRIPT ("  REF_BIN=$main::nodeBin\n");
  } else {
    print $freebayes::SCRIPT ("  REF_BIN=$main::referenceBin\n");
  }
  print $freebayes::SCRIPT ("  REF=$main::reference\n");
  if ($main::modules{$main::task->{TASK}}->{INPUT} eq "node") {
    print $freebayes::SCRIPT ("  if [ ! -d \$REF_BIN ]; then mkdir -p \$REF_BIN; fi\n");
    print $freebayes::SCRIPT ("  if [ ! -f \$REF_BIN/\$REF ]; then rsync $main::referenceBin/\$REF \$REF_BIN; fi\n");
    print $freebayes::SCRIPT ("  if [ ! -f \$REF_BIN/\$REF.fai ]; then rsync $main::referenceBin/\$REF.fai \$REF_BIN; fi\n");
  }
  print $freebayes::SCRIPT ("\n");

  # Write out the command line for freebayes and any associated tools.
  print $freebayes::SCRIPT ("  $main::modules{\"BAMTOOLS\"}->{BIN}/");
  print $freebayes::SCRIPT ("$main::modules{\"BAMTOOLS\"}->{COMMAND} merge \\\n");
  print $freebayes::SCRIPT ("  -region $freebayes::region \\\n");
  foreach my $bamFile (sort @main::completedBamFiles) {print $freebayes::SCRIPT ("  -in $bamFile \\\n");}
  if (! defined $main::noOgap) {print $freebayes::SCRIPT ("  | /share/software/ogap/ogap -f \$REF_BIN/\$REF \\\n");}
  print $freebayes::SCRIPT ("  | /share/home/wardag/programs/freebayes/bin/bamleftalign -f \$REF_BIN/\$REF \\\n");
  if (! defined $main::noBaq) {
    print $freebayes::SCRIPT ("  | $main::modules{\"SAMTOOLS\"}->{BIN}/$main::modules{\"SAMTOOLS\"}->{COMMAND} \\\n");
    print $freebayes::SCRIPT ("  fillmd -Aru - \\\n");
    print $freebayes::SCRIPT ("  \$REF_BIN/\$REF \\\n");
    print $freebayes::SCRIPT ("  2> /dev/null \\\n");
  }
  print $freebayes::SCRIPT ("  | /share/home/wardag/programs/freebayes/bin/freebayes \\\n");
  if (defined $main::exome) {
    print $freebayes::SCRIPT ("  --min-alternate-count 5 \\\n");
  } else {
    print $freebayes::SCRIPT ("  --min-alternate-count 2 \\\n");
  }
  print $freebayes::SCRIPT ("  --min-alternate-qsum 40 \\\n");
  if (! defined $main::noIndels) {print $freebayes::SCRIPT ("  --indels \\\n");}
  if (! defined $main::noMnps) {print $freebayes::SCRIPT ("  --mnps \\\n");}
  print $freebayes::SCRIPT ("  --no-filters \\\n");
  print $freebayes::SCRIPT ("  --haploid-reference \\\n");
  print $freebayes::SCRIPT ("  --genotype-variant-threshold 4 \\\n");
  print $freebayes::SCRIPT ("  --expectation-maximization \\\n");
  print $freebayes::SCRIPT ("  --expectation-maximization-max-iterations 5 \\\n");
  print $freebayes::SCRIPT ("  --genotyping-max-iterations 250 \\\n");
  print $freebayes::SCRIPT ("  --use-best-n-alleles 0 \\\n");
  if (! defined $main::noBinPriors) {print $freebayes::SCRIPT ("  --binomial-obs-priors \\\n");}
  print $freebayes::SCRIPT ("  --allele-balance-priors \\\n");
  print $freebayes::SCRIPT ("  --stdin \\\n");
  print $freebayes::SCRIPT ("  --region $freebayes::region \\\n");
  print $freebayes::SCRIPT ("  --fasta-reference \$REF_BIN/\$REF \\\n");
  #if ($freebayes::refSequence eq "X" && defined $main::cnvBed) {print $freebayes::SCRIPT ("--cnv-map $main::cnvBed \\\n");}
  print $freebayes::SCRIPT ("  --vcf \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $freebayes::SCRIPT ("  > \$OUTPUT_DIR/\$OUTPUT.stdout \\\n");
  print $freebayes::SCRIPT ("  2> \$OUTPUT_DIR/\$OUTPUT.stderr\n\n");

  script_tools::fail(
    $freebayes::SCRIPT,
    "freebayes",
    "\$OUTPUT",
    "\$OUTPUT.stdout",
    "\$OUTPUT.stderr",
    "$main::snpCaller/failed"
  );
  script_tools::copyFiles($freebayes::SCRIPT, 1);
  script_tools::finishScript(
    $freebayes::SCRIPT,
    "$main::outputDirectory/$main::snpCaller",
    $main::snpFileName,
    "");

  # Update the region.
  $main::task->{REGION}++;
  if ($main::task->{REGION} == scalar @target_regions::targetRegions) {general_tools::iterateTask("", \@main::snpCallTasks);}
}
  
1;
