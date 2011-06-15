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
  print("-bq-recal:\t\tAlways use GATK base-quality recalibration - default: do not use for SOLiD reads.\n");
  print("-date:\t\t\tspecify the date to appear in the filenames of outputted files.\n");
  print("  -previousdate:\tprovide a date for the previous index file (incremental alignments).\n");
  print("-divide:\t\tdivide variant calling into specified value in kbp - default: 1000kbp.\n");
  print("-divide-genome:\t\tdivide the genome up by whole genome (w), chromosome (c) or by bed regions (b) - default: chromosome.\n");
  print("-exome:\t\t\tinforms the pipeline that the exome pipeline should be used.\n");
  print("-fastq:\t\t\tdefine a directory where fastq files are stored.\n");
  print("-jobid:\t\t\tspecify a job id that will identify jobs created here.\n");
  print("-index:\t\t\tspecify a sequence index file used for determining alignments.\n");
  print("  -previousindex:\tprevious index file used for incremental alignments.\n");
  print("-local:\t\t\tstore all files on the local disk, nothing on the node.\n");
  print("-memory:\t\t\trequest this much memory from the node.  Format MMmb or MM gb.\n");
  print("-meta:\t\t\tprovide information for alignments in an alternative format to a sequence index file.\n");
  print("-node\t\t\tdefine the node for the jobs to be run on.\n");
  print("-no-baq:\t\t\tdo not use samtools BAQ in the SNP calling pipeline - default is to use.\n");
  print("-no-bin-priors:\t\tdo not use binomial priors in the freebayes SNP calling pipeline - default is to use.\n");
  print("-no-bq-recal:\t\tdo not use base-quality recalibration - default is to use.\n");
  print("-no-indels:\t\tdo not call indels - default is to call.\n");
  print("-no-mnps:\t\tdo not call MNPs - default is to call.\n");
  print("-no-ogap:\t\tdo not use ogap in the SNP calling pipeline - default is to use.\n");
  print("-queue:\t\t\tdefine the queue that the jobs will be sent to - default bigmem for alignment, stage otherwise.\n");
  print("-refseq:\t\tSNP call on this reference sequence only - default: all.\n");
  print("-software:\t\tprovide a list of the paths of the different tools/files - default use hard coded files.\n");
  print("-snp:\t\t\tSNP calling program (freebayes, glfsingle, glfmultiples, none) - default: none.\n");
  print("-threads:\t\trequest this number of threads for alignemnt - default 8.\n");
  print("-user:\t\t\tspecify the user (default: login name).\n");
  print("-wall-time:\t\tDefine a wall time for the job.\n");

  exit(0);
}

# Check that the aligner is defined and that it is valid.
sub checkAligner {
  %main::aligners = (
    'mosaik'       => "1",
    'none'         => "1"
  );

  # If no aligner is being used, and no SNP caller is defined, use
  # freeBayes as the default variant caller..
  if ($main::aligner eq "none" && !defined $main::snpCaller) {
    $main::snpCaller = "freebayes";
  }

  # If no aligner is specified (or specifically not requested), set
  # the default to Mosaik version 2.
  if (! defined $main::aligner) {
    $main::aligner = "mosaik";
    $main::mosaikVersion2 = 1;
  }

  # If the low memory or mosaik version 2 options are set and the
  # specified aligner is not Mosaik, throw an exception.
  if ($main::aligner ne "mosaik" && (defined $main::mosaikVersion2 || defined $main::lowMemory)) {
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("-lowmem and -mosaikv2 options are only valid in conjuction with -aligner mosaik.\n");
    print STDERR ("Error in command_line::checkAligner.\n");
    exit(1);
  }

  # If the requested aligner is not incorporated into the pipeline, 
  # throw an exception.
  if (! exists $main::aligners{$main::aligner} ) {
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("Unknown argument in the -aligner option.\n");
    foreach my $allowedAligner (sort keys %main::aligners) {
      print STDERR ("-aligner $allowedAligner\n");
    }
    print STDERR ("Error in command_line::checkAligner.\n");
    exit(1);
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
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("Unknown argument in the -snp option.\n");
    foreach my $allowedSnpCaller (sort keys %main::snpCallers) {
      print STDERR ("\t-snp $allowedSnpCaller\n");
    }
    print STDERR ("Error in command_line::checkSnpCaller.\n");
    exit(1);
  }
}

# Check target region size for SNP calling.
sub checkTargets {
  if ($main::snpCaller eq "none" && (defined $main::referenceSequence || defined $main::divide) ) {
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("The -refseq and -divide options can only be specified in\n");
    print STDERR ("conjunction with a SNP caller.\n");
    print STDERR ("Error in command_line::checkTargets.\n");
    exit(0);
  }
}

# Chekc the format of the supplied wall time (if necessary).
sub checkWallTime {
  if (defined $main::wallTime) {
    if ($main::wallTime !~ /\d*:\d*:\d*/) {
      print STDERR ("\n***SCRIPT TERMINATED***\n\n");
      print STDERR ("The walltime must have the following format:\n");
      print STDERR ("\tHH:MM:SS\n\n");
      print STDERR ("Error in command_line::checkWallTime\n");
      exit(0);
    }
  }
}

# If a memory requirement is included, check it is of the right
# form.
sub checkMemory {
  if (defined $main::nodeMemory) {
    if ($main::nodeMemory !~ /\d+mb/ && $main::nodeMemory !~ /\d+gb/) {
      print STDERR ("\n***SCRIPT TERMINATED***\n\n");
      print STDERR ("If requesting a memory requirement for the node (-memory), the included ");
      print STDERR ("value must be of the form MMmb or MMgb (e.g. 22gb for Mosaik high memory.\n");
      print STDERR ("Error in command_line::checkMemory.\n");
      exit(1);
    }
  }
}

# Check that the specified bam list contains only bam files, that
# their directories are specified and that they exist.
sub checkBamList {

  # Check that no aligner is requested.  If a bam list is provided, variants
  # will be called on the supplied list and no alignments will be generated.
  if ($main::aligner ne "none") {
    print STDERR ("\n***SCRIPT TERMINATED***\n\n");
    print STDERR ("If a bam list is specified, no alignments can take place (specify -aligner none).\n");
    print STDERR ("Error in command_line::checkBamList.\n");
    exit(1);
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

# Write to screen all the options being used.
sub displayOptions {
  print("The following options/parameters are being used by the pipeline:\n");
  if ($main::aligner eq "none") {print("\tNo alignments being performed.\n");}
  if ($main::aligner eq "mosaik") {
    if (defined $main::mosaikVersion2) {print("\tUsing Mosaik2 to perform alignments.\n");}
    else {print("\tUsing Mosaik1 to perform alignments.\n");}
  }
  if (defined $main::noBaq) {print("\tNot using BAQ in SNP calling.\n");}
  else {print("\tUsing BAQ in SNP calling.\n");}
  print("\n");
}

1;
