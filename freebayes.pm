#!/usr/bin/perl -w

package freebayes;

use strict;
use File::Path qw(make_path);
use File::Copy;

# Use freebayes to perform SNP calling.

sub freebayes {

  # Define the region and the form of the region to be included in
  # the filename.
  $freebayes::region = $target_regions::targetRegions[$main::task->{REGION}];
  ($freebayes::regionFilename = $freebayes::region) =~ s/:/$main::snpDelimiter/g;

  # Generate the file name for the files created in SNP calling.
  if ($main::aligner ne "none") {
    $main::snpFileName=join(".", 
      $main::snpCaller,
      $main::aligner,
      $freebayes::regionFilename,
    );
  } else {
    $main::snpFileName=join(".", 
      $main::snpCaller,
      $freebayes::regionFilename,
    );
  }

  if ($main::jobID) {
    $main::snpFileName = join(".", $main::snpFileName, $main::date, $main::jobID);
  } else {
    $main::snpFileName = join(".", $main::snpFileName, $main::date);
  }
  $freebayes::region =~ s/-/../;
  $freebayes::refSequence = (split(/:/, $freebayes::region))[0];

  if (!defined $main::queue) {$main::queue = "stage";}
  $freebayes::SCRIPT = script_tools::createScript($main::snpFileName, "SNP calling", 1, $main::queue);
  script_tools::scriptFail($freebayes::SCRIPT, $main::snpFileName);
  script_tools::transferFiles($freebayes::SCRIPT);

  print $freebayes::SCRIPT ("# Perform SNP calling with freeBayes\n\n");
  general_tools::setOutputs($freebayes::SCRIPT, $main::snpFileName, "$main::snpFileName.vcf");

  # If improper pairs are to be filtered out, create a json script to
  # be used
  if (!(defined $main::includeImproper && !defined $main::maqQ0)) {createJsonFilter();}

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
  if (!defined $main::includeImproper) {
    print $freebayes::SCRIPT ("$main::modules{\"BAMTOOLS\"}->{COMMAND} filter \\\n");
    print $freebayes::SCRIPT ("  -script $main::outputDirectory/$main::snpCaller/snpFilter.json \\\n");
  } else {
    print $freebayes::SCRIPT ("$main::modules{\"BAMTOOLS\"}->{COMMAND} merge \\\n");
  }
  print $freebayes::SCRIPT ("  -region $freebayes::region \\\n");
  foreach my $bamFile (sort @main::completedBamFiles) {print $freebayes::SCRIPT ("  -in $bamFile \\\n");}
  if (!defined $main::noOgap) {
    print $freebayes::SCRIPT ("  | $main::modules{\"OGAP\"}->{BIN}/$main::modules{\"OGAP\"}->{COMMAND} -f \$REF_BIN/\$REF \\\n");
  }
  print $freebayes::SCRIPT ("  | $main::modules{\"BAM_LEFT_ALIGN\"}->{BIN}/$main::modules{\"BAM_LEFT_ALIGN\"}->{COMMAND} -f \$REF_BIN/\$REF \\\n");
  if (!defined $main::noBaq) {
    print $freebayes::SCRIPT ("  | $main::modules{\"SAMTOOLS\"}->{BIN}/$main::modules{\"SAMTOOLS\"}->{COMMAND} \\\n");
    print $freebayes::SCRIPT ("  fillmd -Aru - \\\n");
    print $freebayes::SCRIPT ("  \$REF_BIN/\$REF \\\n");
    print $freebayes::SCRIPT ("  2> /dev/null \\\n");
  }
  print $freebayes::SCRIPT ("  | $main::modules{\"FREEBAYES\"}->{BIN}/$main::modules{\"FREEBAYES\"}->{COMMAND} \\\n");
  if (defined $main::exome) {
    print $freebayes::SCRIPT ("  --min-alternate-count 5 \\\n");
  } else {
    print $freebayes::SCRIPT ("  --min-alternate-count 2 \\\n");
  }
  print $freebayes::SCRIPT ("  --min-alternate-qsum 40 \\\n");
  if (! defined $main::noIndels) {
    print $freebayes::SCRIPT ("  --indels \\\n");
    print $freebayes::SCRIPT ("  --complex \\\n");
  }
  if (! defined $main::noMnps) {print $freebayes::SCRIPT ("  --mnps \\\n");}
  print $freebayes::SCRIPT ("  --posterior-integration-limits 1,3 \\\n");
  print $freebayes::SCRIPT ("  --genotype-variant-threshold 4 \\\n");
  print $freebayes::SCRIPT ("  --site-selection-max-iterations 5 \\\n");
  print $freebayes::SCRIPT ("  --genotyping-max-iterations 25 \\\n");
  print $freebayes::SCRIPT ("  --no-filters \\\n");
  print $freebayes::SCRIPT ("  --use-best-n-alleles 0 \\\n");
  if (! defined $main::noBinPriors) {print $freebayes::SCRIPT ("  --binomial-obs-priors \\\n");}
  print $freebayes::SCRIPT ("  --allele-balance-priors \\\n");
  print $freebayes::SCRIPT ("  --stdin \\\n");
  print $freebayes::SCRIPT ("  --region $freebayes::region \\\n");
  print $freebayes::SCRIPT ("  --fasta-reference \$REF_BIN/\$REF \\\n");
  #if ($freebayes::refSequence eq "X" && defined $main::cnvBed) {print $freebayes::SCRIPT ("--cnv-map $main::cnvBed \\\n");}
  print $freebayes::SCRIPT ("  --vcf \$OUTPUT_DIR/\$OUTPUT \\\n");
  print $freebayes::SCRIPT ("  --pvar 0.0001 \\\n");
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

# Create a json script to be used for filtering out improper pairs.
sub createJsonFilter {
  open(JSON, ">snpFilter.json");
  print JSON ("{\n");
  print JSON ("\t\"filters\" : [\n");
  if (defined $main::mapQ0) {
    print JSON ("\t\t{ \"id\" : \"mapQuality\",   \"mapQuality\" : \">0\" }\n");
  } else {
    print JSON ("\t\t{ \"id\" : \"mapQuality\",   \"mapQuality\" : \">0\" },\n");
    print JSON ("\t\t{ \"id\" : \"SingleEnd\",    \"isPaired\" : \"false\" },\n");
    print JSON ("\t\t{ \"id\" : \"ProperPaired\", \"isProperPair\" : \"true\" }\n");
  }
  print JSON ("\t],\n");
  print JSON ("\n");
  if (defined $main::mapQ0) {
    print JSON ("\t\"rule\" : \" mapQuality \"\n");
  } else {
    print JSON ("\t\"rule\" : \" ( SingleEnd | ProperPaired ) & mapQuality \"\n");
  }
  print JSON ("}\n");
  close(JSON);

# Check if data was added to the script.  If not, delete the script file.
# If so, move the script into the Scripts directory.
  my $path = "$main::outputDirectory/$main::snpCaller";
  if (! -d $path) {make_path($path);}
  move("snpFilter.json", "$main::outputDirectory/$main::snpCaller/snpFilter.json");
}
  
1;
