#!/usr/bin/perl -w

package reference;

use strict;
use Cwd 'abs_path';

# Check the existence of a user specified reference or set the default.
sub reference {
  if (defined $main::reference) {
    $main::reference           = abs_path($main::reference);
    $main::referenceBin        = $main::reference;
    $main::reference           = (split(/\//,$main::reference))[-1];
    $main::referenceBin        =~ s/\/$main::reference//;
    $main::referenceStub       = $main::reference;

  # Set the default reference to the human reference build 37.
  } else {
    $main::referenceBin  = "/d1/data/references/build_37";
    $main::referenceStub = "human_reference_v37";
  }
  $main::nodeBin             = "$main::scratch/$main::userID/references";
  $main::reference           = "$main::referenceStub.fa";
  $main::referenceDictionary = "$main::referenceStub.dict";

  # Define the neural net generated files for the mapping quality calculations
  # within Mosaik2.  Only check that these files exist if Mosaik2 is the
  # required aligner.
  $main::neuralNetFileSE = "se.100.005.ann";
  $main::neuralNetFilePE = "pe.100.01.ann";
  if ($main::aligner == "mosaik" && $main::mosaikVersion2 == 1) {
    general_tools::checkFileExists("$main::referenceBin/$main::neuralNetFileSE");
    general_tools::checkFileExists("$main::referenceBin/$main::neuralNetFilePE");
  }
  
  # If the reference file does not exist, throw an exception.
  general_tools::checkFileExists("$main::referenceBin/$main::reference");
  general_tools::checkFileExists("$main::referenceBin/$main::referenceDictionary");

  # If alignments are being produced, check that the necessary files
  # exit.
  if ($main::aligner ne "none") {
    if (!defined $main::dbsnp) {
      $main::dbsnpBin = "/d1/data/pipeline_resources/dbSNP";
      $main::dbsnp    = "ALL.wgs.dbsnp.build135.snps.sites.vcf";
    }
    general_tools::checkFileExists("$main::dbsnpBin/$main::dbsnp");
  }
}

# If Mosaik is the aligner being used, check for the existence of
# the Mosaik specific reference files.
sub mosaikReferenceFiles {
  $main::mosaikRef       = "$main::referenceStub\.dat";
  $main::mosaikCSRef     = "$main::referenceStub\_cs.dat";
  $main::mosaikJump      = "$main::referenceStub\_15";
  $main::mosaikJumpCS    = "$main::referenceStub\_cs_15";

  # Check that the necessary files exist.
  general_tools::checkFileExists("$main::referenceBin/$main::mosaikRef");
  general_tools::checkFileExists("$main::referenceBin/$main::mosaikJump\_keys.jmp");
  general_tools::checkFileExists("$main::referenceBin/$main::mosaikJump\_meta.jmp");
  general_tools::checkFileExists("$main::referenceBin/$main::mosaikJump\_positions.jmp");
}

1;
