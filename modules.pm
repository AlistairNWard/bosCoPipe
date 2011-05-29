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

1;
