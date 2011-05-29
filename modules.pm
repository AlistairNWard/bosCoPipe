#!/usr/bin/perl -w

package modules;

use strict;

# All of the different modules that can be called in the execution of the 
# pipeline are defined here.  Each module needs to know the commands used
# to execute programs as well as information on where the module should look
# for and write data to etc.
sub defineModules {
  $main::modules{"BAMTOOLS"} = {
    BIN              => "/share/software/bamtools/bin",
    COMMAND          => "bamtools",
    RETAIN           => "no"
  };
  $main::modules{"INDEX"}          = $main::modules{"BAMTOOLS"};
  $main::modules{"MODIFY_BAM"}     = $main::modules{"BAMTOOLS"};
  $main::modules{"MERGE_BAM"}      = $main::modules{"BAMTOOLS"};
  $main::modules{"BAM_STATISTICS"} = $main::modules{"BAMTOOLS"};

  $main::modules{"RENAME_BAM"} = {
    RETAIN     => "yes",
    INPUT      => "local",
    OUTPUT     => "local",
    DIR        => "merged",
    COPYONFAIL => "no"
  };

  $main::modules{"FASTQVALIDATOR"} = {
    BIN        => "/share/software/fastQValidator/statgen/src/bin",
    COMMAND    => "fastQValidator",
    RETAIN     => "yes",
    INPUT      => "local",
    OUTPUT     => "node",
    DIR        => "",
    COPYONFAIL => ""
  };

  $main::modules{"BQ_RECALIBRATION"} = {
    BIN              => "/share/software/GenomeAnalysisToolKit/GenomeAnalysisTK-1.0.5506",
    PRE_COMMAND      => "java -Xmx8g -jar",
    COMMAND          => "GenomeAnalysisTK.jar",
    RETAIN           => "no",
    INPUT            => "local",
    OUTPUT           => "node",
    DIR              => "merged",
    COPYONFAIL       => "no"
  };

  $main::modules{"DUPLICATE_MARK_PICARD"} = {
    BIN         => "/share/software/picard/picard-tools-1.12",
    PRE_COMMAND => "java -Xmx2g -jar",
    COMMAND     => "MarkDuplicates.jar",
    RETAIN      => "no",
    INPUT       => "local",
    OUTPUT      => "node",
    DIR         => "merged",
    COPYONFAIL  => "no"
  };

  $main::modules{"DUPLICATE_MARK_BCM"} = {
    BIN              => "/share/software/picard/picard-tools-1.12",
    PRE_COMMAND      => "java -classpath",
    COMMAND          => "sam-1.12.jar",
    COMMAND_MODIFIER => "BCMMarkDupes",
    RETAIN           => "no",
    INPUT            => "local",
    OUTPUT           => "node",
    DIR              => "merged",
    COPYONFAIL       => "no"
  };

  $main::modules{"SAMTOOLS"} = {
    BIN              => "/share/software/samtools/samtools-0.1.12a",
    COMMAND          => "samtools",
    RETAIN           => "yes",
    INPUT            => "local",
    OUTPUT           => "node",
    DIR              => "glf",
    COPYONFAIL       => "no"
  };
  
  $main::modules{"DUPLICATE_MARK_PICARD"} = {
    BIN         => "/share/software/picard/picard-tools-1.12",
    PRE_COMMAND => "java -Xmx2g -jar",
    COMMAND     => "MarkDuplicates.jar",
    RETAIN      => "no",
    INPUT       => "local",
    OUTPUT      => "node",
    DIR         => "merged",
    COPYONFAIL  => "no"
  };

  $main::modules{"DUPLICATE_MARK_BCM"} = {
    BIN              => "/share/software/picard/picard-tools-1.12",
    PRE_COMMAND      => "java -classpath",
    COMMAND          => "sam-1.12.jar",
    RETAIN           => "no",
    INPUT            => "local",
    OUTPUT           => "node",
    DIR              => "merged",
    COPYONFAIL       => "no"
  };

# Now build aligner specific modules.
#
# Mosaik.
  if ($main::aligner eq "mosaik") {

    # Version 1 modules.
    if (! defined $main::mosaikVersion2) {
      $main::modules{"MOSAIKBUILDV1"} = {
        BIN        => "/share/home/wardag/programs/Mosaik/1.1.0005b/1.1.0005b/bin",
        COMMAND    => "MosaikBuild",
        RETAIN     => "no",
        INPUT      => "local",
        OUTPUT     => "node",
        DIR        => "build",
        COPYONFAIL => "no"
      };

      $main::modules{"MOSAIKALIGNERV1"} = {
        BIN        => "/share/home/wardag/programs/Mosaik/1.1.0005b/1.1.0005b/bin",
        COMMAND    => "MosaikAligner",
        RETAIN     => "no",
        INPUT      => "local",
        OUTPUT     => "node",
        DIR        => "aligner",
        COPYONFAIL => "no"
      };

      $main::modules{"MOSAIKSORT"} = {
        BIN        => "/share/home/wardag/programs/Mosaik/1.1.0005b/1.1.0005b/bin",
        COMMAND    => "MosaikSort",
        RETAIN     => "no",
        INPUT      => "node",
        OUTPUT     => "node",
        DIR        => "sort",
        COPYONFAIL => "no"
      };

      $main::modules{"MOSAIKTEXT"} = {
        BIN        => "/share/home/wardag/programs/Mosaik/1.1.0005b/1.1.0005b/bin",
        COMMAND    => "MosaikText",
        RETAIN     => "no",
        INPUT      => "node",
        OUTPUT     => "node",
        DIR        => "bam",
        COPYONFAIL => "no"
      };

    # Mosaik version 2.
    } else {
      $main::modules{"MOSAIKBUILDV2"} = {
        BIN        => "/share/home/wardag/programs/Mosaik/Mosaik.2.0.113/bin",
        COMMAND    => "MosaikBuild",
        RETAIN     => "no",
        INPUT      => "local",
        OUTPUT     => "node",
        DIR        => "build",
        COPYONFAIL => "no"
      };

      $main::modules{"MOSAIKALIGNERV2"} = {
        BIN        => "/share/home/wardag/programs/Mosaik/Mosaik.2.0.113/bin",
        COMMAND    => "MosaikAligner",
        RETAIN     => "no",
        INPUT      => "local",
        OUTPUT     => "node",
        DIR        => "aligner",
        COPYONFAIL => "no"
      };

      $main::modules{"SORT_MOSAIKV2"} = {
        RETAIN     => "yes",
        INPUT      => "node",
        OUTPUT     => "node",
        DIR        => "bam",
        COPYONFAIL => "no"
      };
    }

  # BWA.
  } elsif ($main::Aligner =~ /bwa/i) {

  # No aligner.
  } elsif ($main::Aligner =~ /none/i) {
  } else {
    command_line::checkAligner();
  }

  # Now build SNP caller specific modules.
  $main::modules{"FREEBAYES"} = {
    BIN        => "/share/home/wardag/programs/freebayes/bin",
    COMMAND    => "freebayes",
    RETAIN     => "yes",
    INPUT      => "local",
    OUTPUT     => "local",
    DIR        => "freebayes",
    COPYONFAIL => "no"
  };
}

# Read in the paths of the software components from the supplied file (if
# necessary).
sub readSoftware() {
  my @array;
  my ($tool, $path);
  my (%allowedTools, %allowedFiles);

  # Define a list of allowed tools and files.
  %allowedTools = ('BAMTOOLS', 1,
                   'FASTQVALIDATOR', 1,
                   'FREEBAYES', 1,
                   'GATK', 1,
                   'MOSAIK', 1,
                   'PICARD', 1,
                   'SAMTOOLS', 1);

  # Now include the allowed files.
  %allowedFiles = ('DBSNP', 1,
                   'REF', 1);

  if (defined $main::softwareList) {
    open(SOFTWARE, "<$main::softwareList") || die("Failed to open file: $main::softwareList");
    while(<SOFTWARE>) {
      chomp;
      if (/\S+:\S+/) {
        @array = split(/:/, $_);
        $tool = $array[0];
        $path = $array[1];

        # Check that the tool is allowed.
        if (!exists $allowedTools{$tool} && !exists $allowedFiles{$tool}) {
          print STDERR ("\n***SCRIPT TERMINATED***\n\n");
          print STDERR ("Incorrect format in string.  Entries must be of the form:\n");
          print STDERR ("\t<TOOL>:<PATH>\n");
          print STDERR ("\t<FILENAME>:<PATH>/<FILE>\n\n");
          print STDERR ("Allowed tools include:\n");
          foreach my $key (keys %allowedTools) {print STDERR ("\t$key\n");}
          print STDERR ("\n");
          print STDERR ("Allowed files include:\n");
          foreach my $key (keys %allowedFiles) {print STDERR ("\t$key\n");}
          print("\n");
          print STDERR ("Error in modules::readSoftware.\n");
          exit(1);
        }

        # Strip off trailing /, if exists.
        if ($path =~ /\/$/) {$path = substr($path, 0, -1);}

        # Update the tool with the path given here.
        if ($tool =~ /^DBSNP$/) {
          $main::dbsnp    = $path;
          $main::dbsnpBin = $path;
          $main::dbsnp    = (split(/\//, $main::dbsnp))[-1];
          $main::dbsnpBin =~ s/\/$main::dbsnp//;
        } elsif ($tool =~ /^REF$/) {
          $main::reference     = $path;
          $main::referenceBin  = $path;
          $main::reference     = (split(/\//, $main::reference))[-1];
          $main::referenceBin  =~ s/\/$main::reference//;
          $main::referenceStub = $main::reference;
        } else {
          $main::modules{$tool}->{BIN} = $path;
        }
      } else {
        print STDERR ("\n***SCRIPT TERMINATED***\n\n");
        print STDERR ("Incorrect format in string.  Entries must be of the form:\n");
        print STDERR ("\t<TOOL>:<PATH>\n");
        print STDERR ("\t<FILENAME>:<PATH>/<FILE>\n\n");
        print STDERR ("Allowed tools include:\n");
        foreach my $key (keys %allowedTools) {print STDERR ("\t$key\n");}
        print STDERR ("\n");
        print STDERR ("Allowed files include:\n");
        foreach my $key (keys %allowedFiles) {print STDERR ("\t$key\n");}
        print STDERR ("Error in modules::readSoftware.\n");
        print("\n");
        exit(1);
      }
    }
  }
}

1;
