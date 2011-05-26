#!/usr/bin/perl

use strict;
use Getopt::Long;
use Cwd 'abs_path';

# Record the version number.

$main::version="2.001";
$main::versionDate="May 2011";

# Define required files and packages.

$main::codeDir="/share/home/wardag/Pipeline/pipeline.v2";

require "$main::codeDir/bamtools.pm";
require "$main::codeDir/command_line.pm";
require "$main::codeDir/create_scripts.pm";
require "$main::codeDir/freebayes.pm";
require "$main::codeDir/general_tools.pm";
require "$main::codeDir/merge_tools.pm";
require "$main::codeDir/modules.pm";
require "$main::codeDir/mosaik.pm";
require "$main::codeDir/reference.pm";
require "$main::codeDir/script_tools.pm";
require "$main::codeDir/search.pl";
require "$main::codeDir/sequence_index.pm";
require "$main::codeDir/software.pm";
require "$main::codeDir/target_regions.pm";
require "$main::codeDir/tools.pm";

# Set buffer to flush after every print statement.

$| = 1;

# Define some global variables.

$main::cwd = abs_path(".");

# Find the current time.  This will be used in generating
# file names as well as for printout to output files.

($main::time,$main::day,$main::month,$main::year)=general_tools::findTime();

# Check command line arguments and throw an exception if
# information is missing or incorrect.

GetOptions('aligner=s'       => \$main::aligner,
           'bamdir=s'        => \$main::bamDirectory,
           'bamlist=s'       => \$main::bamList,
           'date=s'          => \$main::date,
           'dir=s'           => \$main::outputDirectory,
           'divide:i'        => \$main::targetRegionSize,
           'divide-genome:s' => \$main::divideGenome,
           'exome'           => \$main::exome,
           'fastq=s'         => \$main::fastqDirectory,
           'index=s'         => \$main::indexFile,
           'jobid=s'         => \$main::jobID,
           'lowmem'          => \$main::lowMemory,
           'meta=s'          => \$main::metaData,
           'previousdate=s'  => \$main::previousDate,
           'previousindex=s' => \$main::previousIndex,
           'snp=s'           => \$main::snpCaller,
           'mosaikv2'        => \$main::mosaikVersion2,
           'nobaq'           => \$main::noBaq,
           'no-bin-priors'   => \$main::noBinPriors,
           'noindels'        => \$main::noIndels,
           'noogap'          => \$main::noOgap,
           'nomnps'          => \$main::noMnps,
           'reference=s'     => \$main::reference,
           'refseq=s'        => \$main::referenceSequence,
           'user=s'          => \$main::userID,
           'h|help|?'        => \$main::help)
           || command_line::pipelineHelp();

# Print the version number to screen.

print("\n=======================================\n");
print("Boston College variant calling pipeline\n");
print("Version $main::version - $main::versionDate\n\n$main::time\n");
print("=======================================\n\n");

# Provide usage information if the help option was specified.
if (defined $main::help) {command_line::pipelineHelp();}

# Initialise variables.
general_tools::initialise();

# Determine the user ID if not specified.
if (! defined $main::userID) {$main::userID = getlogin();}

# Check that the specified aligner and associated options are valid.
command_line::checkAligner();

# Check that the specified SNP caller and associated options are valid.
command_line::checkSnpCaller();

# If SNP calling, determine reference sequences to call on and what
# size chunks to call on.
command_line::checkTargets();

# If a reference is specified, check that it exists as well as the jump
# database if Mosaik is being used.  If not, set the default.
reference::reference();

# If a bam list is defined, check that it exists, that all of the
# contents are bam files.  If so, populate @main::bamList with the
# list of bam files.
if (defined $main::bamList) {command_line::checkBamList();}

# Define the software tools that can be run in the pipeline.
# For each tools, a list of parameters is also defined and the
# files that in creates are listed.  If new tools are added to
# the pipeline, a new routine to run the package is required as
# well as an entry in this routine.  Everything else will be
# automatically dealt with.
modules::defineModules();
software::aligners();
software::mergePipeline();
software::snpCallers();
target_regions::defineRegions(); # target_regions.pl

# Check if a date has been supplied.  If not, the current date will be
# used in the generated filenames.
command_line::checkDate();

# If an older sequence.index file is provided, a search for release
# bams from the older sequence.index file will be performed.  This
# search requires knowledge of the date of the previous index file.
# Thus, the old index file and the corresponding date must BOTH be
# provided.  This is only required if alignments are being performed.
#if ($main::aligner ne "none") {CommandLine::IncrementalIndex();}

# If a previous sequence index is provided along with a current index file,
# read through the 'old' sequence.index file and build a hash table of the
# md5 check sums for the fastq files.  Skip this step if running from a
# supplied bam list.
if (! defined $main::bamList) {
  if (defined $main::previousIndex) {
    $main::previousIndex = abs_path($main::previousIndex);
    sequenceIndex::parsePreviousIndexFile();
  }

# Read metadata to determine the parameters and fastq files to
# input into the pipeline.  If a sequence.index file exists, this
# will be read.  The entire file is parsed prior to creating any scripts.
# This is to ensure that all files associated with each run are
# accounted for and the relevant single-end, paired-end or both
# pipeline scripts can be created.
  if (defined $main::indexFile) {
    $main::indexFile = abs_path($main::indexFile);
    sequence_index::parseIndexFile();
  }
  elsif (defined $main::metaData) {
    $main::metaData = abs_path($main::metaData);
    sequence_index::metaData();
  }
  else {
    print("\n***SCRIPT TERMINATED***\n");
    print("A sequence index or meta data file must be provided.\n");
    die("Error in pipeline_main.pl.\n");
  }
}

# Define the name of the directory to keep all of the files generated by
# the pipeline.
general_tools::generateDirectory();

# Search for files already existing within the directory structure.  Only search
# for files created by the defined aligner.  Do not perform this search if a
# list of bam files has been provided.
if (! defined $main::bamList) {
  print("Searching for existing files...");
  find(\&fileSearch, $main::cwd); # Search.pl
  print("done.\n");

# If a separate fastq directory is defined, also search within this directory.
  if (defined $main::fastqDirectory) {
    print("Searching for fastq files...");
    $main::fastqDirectory = abs_path($main::fastqDirectory);
    find(\&findFastq, $main::fastqDirectory); # Search.pl
    print("done.\n");
  }

# Now search for previous bam files in the specified bam directory.
# This step is only performef if the directory is specified.
  if (defined $main::bamDirectory) {
    $main::bamDirectory = abs_path($main::bamDirectory);
    print("Searching for previous release bam files...");
    find(\&fileSearch,$main::bamDirectory); # Search.pl
    print("done.\n");
  }
} else {
  foreach my $bam (@main::bamList) {push(@main::completedBamFiles, $bam);}
}

# Ceunt the number of samples and sample x technology pairs that will be
# analysed by the pipeline.
merge_tools::countSamples();

# Perform a Check for each of the merged bam files.  Specifically, if multiple
# bam files exist (i.e. different dates), choose in the following order:
#   1: Current date
#   2: Supplied previous date (if exists).
#   3: Whichever bam file is left (i.e. neither of these dates).
#
# If no bam file exists, mark the MergeInfo info string with the tag,
# exists:no.  Otherwise include the tags: exists:yes, path:<path> and
# file:<file>.
#
# If there is an extant file, check the following aspects:
#   1: Are all runs in MergeInfo included in the file?
#   2: Are any other runs found in the file (e.g. withdrawn runs)?
#   3: Have the md5 tags of the runs changed?
merge_tools::selectMergedBam();
merge_tools::interrogateMergedBamFile();

# Check the arrays of failed jobs and include an additional status tag.
create_scripts::checkFailed();

# Now work through each sample x technology pair in turn, creating all of the
# necessary script files.
if ($main::aligner ne "none") {
  create_scripts::createScripts();
  create_scripts::printScriptStatistics();
} else {
  foreach my $bam (@main::currentIncrementBams) {push(@main::completedBamFiles, $bam);}
  foreach my $bam (@main::previousIncrementBams) {push(@main::completedBamFiles, $bam);}
  foreach my $bam (@main::previousBams) {push(@main::completedBamFiles, $bam);}
  foreach my $file (@main::existingFiles) {if ($file =~ /\.bam$/) {push(@main::completedBamFiles, $file);}}
}

# Now create SNP calling scripts if required.
if ($main::snpCaller ne "none") {
  create_scripts::snpScripts();
}
