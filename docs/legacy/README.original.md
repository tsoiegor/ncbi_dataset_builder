# Multi-Species ATAC-seq Data Processing Pipeline

![image info](ATACseq_pipeline.png)

## Overview

This repository contains a comprehensive pipeline for downloading, processing, and analyzing multi-species ATAC-seq data from the NCBI SRA database. The pipeline handles everything from metadata retrieval to genome alignment and BigWig file generation.

## Project Structure

### Core Modules

1. **DescriptionCollection.py** - Handles loading and processing of sample metadata from JSON files
2. **DownloadSRA.py** - Downloads SRA data using NCBI's prefetch tool
3. **NCBI.py** - Interfaces with NCBI to retrieve sample descriptions and taxonomy information
4. **SRAinfo.py** - Main class for filtering, processing, and analyzing SRA metadata
5. **GenomeUtils.py** - Downloads genomes from NCBI and DNA Zoo
6. **GenomeCollection.py** - Processes and organizes genome files from multiple sources

### Pipeline Scripts

7. **generateBatches.py** - Creates processing batches based on sample metadata
8. **downloadBatch.py** - Downloads SRA data for a specific batch
9. **downloadGenomes.py** - Downloads genomes for species in the dataset
10. **runDatasetGeneration.py** - Main pipeline script for processing ATAC-seq data
11. **runDatasetGeneration.sh** - bash script for running runDatasetGeneration.py with recommended params

## Key Features

- **Multi-species Support**: Processes ATAC-seq data across various species
- **Metadata Enrichment**: Retrieves and integrates sample descriptions, tissue types, and taxonomy
- **Smart Filtering**: Filters samples based on quality metrics (read count, library type, etc.)
- **Batch Processing**: Handles large datasets through parallel batch processing
- **Genome Management**: Downloads and processes reference genomes from multiple sources
- **Quality Control**: Includes FASTQ processing, deduplication, and alignment statistics
- **Output Generation**: Produces standardized BAM and BigWig files

## Installation

### Prerequisites

```bash
# Required Python packages
polars
matplotlib
seaborn
boto3
beautifulsoup4
urllib3

# Required bioinformatics tools
prefetch (from SRA Toolkit)
fasterq-dump
bowtie2
samtools
fastp
bamCoverage (from deepTools)
pigz
```

### Setup

```bash
# Clone the repository
git clone https://github.com/tsoiegor/multispeciesATACseq.git
cd multispeciesATACseq

# Install Python dependencies
pip install polars matplotlib seaborn boto3 beautifulsoup4 urllib3

# Install bioinformatics tools (via conda recommended)
conda install -c bioconda sra-tools bowtie2 samtools fastp deeptools pigz
```

## Usage

### 1. Split samples into batches and generate Metadata

```bash
# Generate batch files (writes to default dirs)
python generateBatches.py
```
```bash
# Generate sra , biosample and merged metadata (description) files (writes to default dirs)
python generateDescriptions.py
```


### 2. Download Genomes

```bash
# Download genomes for all species in the dataset
python downloadGenomes.py --SRAinfo path/to/SraRunInfo.csv --rootDIR path/to/genomes --source both
```

### 3. Process Genome Collection

```bash
# Process and organize downloaded genomes
python GenomeCollection.py --inputGenomeDirs path/to/genomes --outputGenomeDir path/to/processed_genomes --tmp path/to/tmp
```

### 4. Run Main Pipeline

```bash
# Run with recommended params
bash runDatasetGeneration.sh

# Or directly with Python
python runDatasetGeneration.py \
    --dataDIR /path/to/raw_data \
    --genomesDIR /path/to/genomes \
    --outputDIR /path/to/output \
    --CPU 32 \
    --MEM 500G \
    --libsource bulk \
    --minSpots 50000000 \
    --minAvgLength 100
```

## Configuration Options

### Key Parameters for `runDatasetGeneration.py`

| Parameter | Description | Default |
|-----------|-------------|---------|
| `--dataDIR` | Directory for raw SRA data | Required |
| `--genomesDIR` | Directory containing reference genomes | Required |
| `--outputDIR` | Output directory for processed files | Required |
| `--CPU` | Number of CPU threads | 90 |
| `--MEM` | Memory allocation | 1000G |
| `--libsource` | Library type (`bulk` or `sc`) | `bulk` |
| `--minSpots` | Minimum read count filter | 50,000,000 |
| `--minAvgLength` | Minimum average read length | 100 |
| `--minYear` | Earliest release year | 2000 |
| `--resumeLastBatch` | Resume from last completed batch | False |

### Filtering Parameters in `SRAinfo.py`

The pipeline includes sophisticated filtering capabilities:

- **Quality filtering**: By read count, read length, and file size
- **Species filtering**: Excludes improperly named species
- **Tissue filtering**: Standardizes tissue annotations
- **Taxonomy filtering**: Groups by taxonomic class with sample limits
- **Tumor filtering**: Option to exclude tumor samples

## Output Structure

```
output_directory/
├── Genomes/                 # Indexed genomes for aligner
├── BigWigs/                # BigWig coverage files
├── ChromSizes/             # Chromosome size files
├── fileMappings/           # sample to description file and BigWig files (foward and reverse) mappings
├── alignmentStats/         # Alignment statistics + fastq stats (json + html)
├── batchFiles/            # json files with sample to SRR list mapping. Size of SRR files is limited to max bath size param
└── sampleDescriptions/    # Sample metadata retrieved from NCBI: from srr page and biosample page
```

## Workflow Details

### 1. Metadata Processing
- Loads SRA run information
- Filters by library type and quality metrics
- Adds taxonomic classification
- Adds tissue annotations
- Groups by biological sample

### 2. Batch Generation
- Creates manageable processing batches (~350GB each)
- Maximizes tissue coverage across species
- Balances sample distribution

### 3. Data Download
- Downloads SRA files using prefetch
- Extracts FASTQ files with fasterq-dump
- Processes with fastp for quality control

### 4. Alignment
- Builds genome indexes with bowtie2
- Aligns reads with quality filtering
- Generates BAM files with alignment statistics

### 5. Coverage Analysis
- Converts BAM to strand-specific BigWig files
- Generates chromosome size files
- Creates file mappings 
