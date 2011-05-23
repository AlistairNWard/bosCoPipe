#!/usr/bin/perl -w

package command_line;

use strict;
use Cwd 'abs_path';

# Print usage to screen.
sub pipelineHelp {
  print("\nUsage: perl Pipeline.pl [options]\n\n");
  print("Options:\n\n");
  print("-aligner:\t\talignment program (mosaik, bwa, none) - default: mosaik.\n");
  print("  -lowmem:\t\tuse low memory version (Mosaik only).\n");
  print("  -mosaikv2:\t\tuse Mosaik version 2 (Mosaik only - default).\n");
  print("-bamlist:\t\tspecify a list of bam files to use for variant calling.\n");
  print("-bamdir:\t\tspecify a directory where previously aligned bam files reside.\n");
  print("-date:\t\t\tspecify the date to appear in the filenames of outputted files.\n");
  print("  -previousdate:\tprovide a date for the previous index file (incremental alignments).\n");
  print("-divide:\t\tdivide SNP calling into specified value in kbp - default: 1000kbp.\n");
  print("-exome:\t\t\tinforms the pipeline that the exome pipeline should be used.\n");
  print("-fastq:\t\t\tdefine a directory where fastq files are stored.\n");
  print("-jobid:\t\t\tspecify a job id that will identify jobs created here.\n");
  print("-index:\t\t\tspecify a sequence index file used for determining alignments.\n");
  print("  -previousindex:\tprevious index file used for incremental alignments.\n");
  print("-meta:\t\t\tprovide information for alignments in an alternative format to a sequence index file.\n");
  print("-refseq:\t\tSNP call on this reference sequence only - default: all.\n");
  print("-snp:\t\t\tSNP calling program (freebayes, glfsingle, glfmultiples, none) - default: none.\n");
  print("-user:\t\t\tspecify the user (default: login name).\n");

  exit(0);
}

# Check that the aligner is defined and that it is valid.
sub checkAligner {
  %main::aligners = (
    'mosaik'       => "1",
    'none'         => "1"
  );

  # If no aligner is specified (or specifically not requested), set
  # the default to Mosaik version 2.
  if (! defined $main::aligner) {
    $main::aligner = "mosaik";
    $main::mosaikVersion2 = 1;
  }

  # If the low memory or mosaik version 2 options are set and the
  # specified aligner is not Mosaik, throw an exception.
  if ($main::aligner ne "mosaik" && (defined $main::mosaikVersion2 || defined $main::lowMemory)) {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("-lowmem and -mosaikv2 options are only valid in conjuction with -aligner mosaik.\n");
    die("Error in command_line::checkAligner.\n");
  }

  # If the requested aligner is not incorporated into the pipeline, 
  # throw an exception.
  if (! exists $main::aligners{$main::aligner} ) {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("Unknown argument in the -aligner option.\n");
    foreach my $allowedAligner (sort keys %main::aligners) {
      print("-aligner $allowedAligner\n");
    }
    die("Error in command_line::checkAligner.\n");
  }
}

# Check that the SNP caller is defined and that it is valid.
sub checkSnpCaller {
  %main::snpCallers = (
    "freebayes"    => "1",
    "glfSingle"    => "1",
    "glfMultiples" => "1",
    "none"         => "1",
  );

  # If no SNP caller is defined, default to none.
  if (! defined $main::snpCaller) {$main::snpCaller = 'none';}

  if (! exists $main::snpCallers{$main::snpCaller} ) {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("Unknown argument in the -snp option.\n");
    foreach my $allowedSnpCaller (sort keys %main::snpCallers) {
      print("\t-snp $allowedSnpCaller\n");
    }
    die("Error in command_line::checkSnpCaller.\n");
  }
}

# Check target region size for SNP calling.
sub checkTargets {
  if ($main::snpCaller eq "none" && (defined $main::referenceSequence || defined $main::divide) ) {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("The -refseq and -divide options can only be specified in\n");
    print("conjunction with a SNP caller.\n");
    die("Error in command_line::checkTargets.\n");
  }

  if ($main::snpCaller ne "none") {
    if (!defined $main::divide) {$main::divide = 100;}
  }
}

# Check that the specified bam list contains only bam files, that
# their directories are specified and that they exist.
sub checkBamList {

  # Check that no aligner is requested.  If a bam list is provided, variants
  # will be called on the supplied list and no alignments will be generated.
  if ($main::aligner ne "none") {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("If a bam list is specified, no alignments can take place (specify -aligner none).\n");
    die("Error in command_line::checkBamList.\n");
  } else {
    $main::Aligner = "none";
  }

  # Check if the list exists.
  if (! -f $main::bamList) {
    print("\n***SCRIPT TERMINATED***\n\n");
    print("The file $main::bamList does not exist.\n");
    die("Error in command_line::checkBamList.\n");

  # Check the contents of the list.
  } else {
    open(BAMLIST,"<$main::bamList");
    while(<BAMLIST>) {
      chomp;
      if (! -f $_) {
        print("\n***SCRIPT TERMINATED***\n\n");
        print("The bam file $_ does not exist or does not have the full path included.\n");
        die("Error in command_line::checkBamList.\n");
      } else{
        push(@main::bamList, abs_path($_));
      }
    }
  }
}

# If a date has been included, check that it conforms to the
# format YYYYMMDD.  If a date has not been supplied, use the
# current date instead.
sub checkDate {
  my @months=qw(Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec);

  if (defined $main::date) {

  # Check the length of the date.  It should be 8 characters long.
    my $date_length = length($main::date);
    if ($date_length ne 8) {
      print("\n***SCRIPT TERMINATED***\n");
      print("Requested date does not conform to the required format (YYYYMMDD).\n");
      die("Error in command_line::checkDate.\n");;
    }
    $main::indexYear  = substr($main::date, 0, 4);
    $main::indexMonth = substr($main::date, 4, 2);
    $main::indexDay   = substr($main::date, 6, 2);

    if ($main::indexMonth > 12 || $main::indexMonth < 1) {
      print("\n***SCRIPT TERMINATED***\n");
      print("Requested date includes an invalid month ($main::indexMonth).\n");
      die("Error in command_line::checkDate.\n");;
    }
    print("Using the following date in the created files:\n\t");
  } else {
    $main::date  = sprintf("%04d%02d%02d", $main::year, $main::month, $main::day);
    $main::indexYear  = $main::year;
    $main::indexMonth = $main::month;
    $main::indexDay   = $main::day;
    print("WARNING: No date set for the filenames.  Using the current date:\n\t");
  }
  my $month = $months[$main::indexMonth-1];
  print("$main::indexDay $month $main::indexYear ($main::date)\n\n");

  # If incremental alignments are being performed, check the format of the
  # previous date.
  if (defined $main::previousDate) {

# Check the length of the date.  It should be 8 characters long.
    my $date_length = length($main::previousDate);
    if ($date_length ne 8) {
      print("\n***SCRIPT TERMINATED***\n");
      print("Requested date (for previous index file) does not conform to the required format (YYYYMMDD).\n");
      die("Error in command_line::checkDate.\n");;
    }
    $main::indexYear  = substr($main::previousDate, 0, 4);
    $main::indexMonth = substr($main::previousDate, 4, 2);
    $main::indexDay   = substr($main::previousDate, 6, 2);

    if ($main::indexMonth > 12 || $main::indexMonth < 1) {
      print("\n***SCRIPT TERMINATED***\n");
      print("Requested date (for previous index file) includes an invalid month ($main::indexMonth).\n");
      die("Error in command_line::checkDate.\n");;
    }
  }

}

1;
