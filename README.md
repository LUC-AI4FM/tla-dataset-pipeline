# TLA Dataset Pipeline

![Lint and Format](https://github.com/LUC-FMitF/tla-dataset-pipeline/actions/workflows/ci.yaml/badge.svg)
![Nightly Discovery](https://github.com/LUC-FMitF/tla-dataset-pipeline/actions/workflows/nightly_discovery.yaml/badge.svg)

## Overview

A Python pipeline for discovering, extracting, and parsing TLA+ specification files from GitHub repositories. The pipeline:

1. **Discovers** TLA+ repositories using GitHub search and seed lists
2. **Extracts** `.tla`, `.cfg`, and `.tlaps` files from discovered repositories
3. **Parses** TLA+ specifications using LLM-based analysis
4. **Uploads** results to AWS S3 for archival and analysis

## Installation

### Prerequisites

- Python 3.10 or higher
- pip (Python package manager)
- Git

### Install Steps

1. Clone the repository:

```bash
git clone https://github.com/LUC-FMitF/tla-dataset-pipeline.git
cd tla-dataset-pipeline
```

2. Create a virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install the package with core dependencies:

```bash
pip install -e .
```

3. For S3 support, install with the s3 extra:

```bash
pip install -e ".[s3]"
```

4. For full functionality (including LLM parsing):

```bash
pip install -e ".[all]"
```

## Configuration

### GitHub Authentication

Required for discovery and extraction steps.

```bash
export GITHUB_TOKEN=your_github_personal_access_token
```

### AWS Setup

#### Option 1: AWS Environment Variables

Required for pushing data to S3. Set these in your shell or `.env` file:

```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-2  # or your preferred region
```

#### Option 2: AWS CLI Configuration

Alternatively, configure AWS credentials using the AWS CLI:

```bash
aws login # Follow prompts to login
```

This stores credentials in `~/.aws/credentials` and region in `~/.aws/config`.

### Configuration File (Optional)

Create a `.env` file in the project root for local development:

```bash
GITHUB_TOKEN=your_token
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_DEFAULT_REGION=us-east-2
```

## Running the Pipeline

### Discovery Pipeline

Discover TLA+ repositories from GitHub:

```bash
# Run full discovery (seeds + search + validate)
tladata discover

# Or run individual steps:
tladata fetch-seeds          # Fetch seeded repositories only
tladata search              # Run search queries
tladata validate <path>     # Validate a manifest
```

### Extraction Pipeline

Extract TLA+ files from discovered repositories:

```bash
# Extract files from the default manifest
tladata pull

# Specify custom manifest and output directory
tladata pull --manifest custom_manifest.jsonl --output extracted_files/
```

This creates a directory structure like:

```
data/raw/
  owner-repo-name/
    file1.tla
    file2.cfg
    file3.tlaps
  another-repo/
    ...
```

### Parsing Pipeline

Parse TLA+ files using LLM analysis:

```bash
# Parse a single file with default model (gpt-4)
tladata parse data/raw/some-repo/example.tla

# Parse a directory with custom model and version
tladata parse data/raw/ --model openai:gpt-4o --version 3

# Skip existing results and reprocess
tladata parse data/raw/ --no-skip
```

### Full Pipeline on Linux Server

Run the complete pipeline from discovery to S3 upload:

```bash
# 1. Set up environment
export GITHUB_TOKEN=your_github_token
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_DEFAULT_REGION=us-east-2

# 2. Navigate to project directory
cd /path/to/tla-dataset-pipeline

# 3. Run discovery
tladata discover

# 4. Extract files
tladata pull --manifest manifests/sources/sources_latest.jsonl --output data/raw

# 5. Parse files (optional, requires LLM API keys)
tladata parse data/raw --model openai:gpt-4

# 6. Push to S3 (see next section)
tladata push-to-s3 --input data/raw --bucket your-bucket-name --prefix raw
```

## Pushing Data to AWS S3

### Basic Upload

Push extracted files to S3:

```bash
tladata push-to-s3 --input data/raw --bucket my-tla-bucket
```

This uploads all files from `data/raw/` to `s3://my-tla-bucket/raw/`

### Upload with Custom Configuration

```bash
# Specify S3 prefix (folder structure)
tladata push-to-s3 \
  --input data/raw \
  --bucket my-tla-bucket \
  --prefix tla-extracted/v1

# Upload to different region
tladata push-to-s3 \
  --input data/raw \
  --bucket my-tla-bucket \
  --prefix raw \
  --region us-west-2

# Upload with manifest
tladata push-to-s3 \
  --input data/raw \
  --bucket my-tla-bucket \
  --prefix raw \
  --manifest-bucket my-tla-bucket \
  --manifest-prefix manifests/sources
```

### Dry Run (Preview)

Preview what would be uploaded without actually uploading:

```bash
tladata push-to-s3 \
  --input data/raw \
  --bucket my-tla-bucket \
  --prefix raw \
  --dry-run
```

### S3 Directory Structure

Files are uploaded preserving their local structure. Example:

```text
Local: data/raw/
  kubernetes/scheduler/
    lock.tla

AWS S3: s3://my-bucket/raw/
  kubernetes/scheduler/
    lock.tla
```

## Troubleshooting

### GitHub Token Issues

```bash
# Verify token is set
echo $GITHUB_TOKEN

# Create a new token at https://github.com/settings/tokens
```

### AWS Credentials Not Found

```bash
# Check credentials are set
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY

# Or check AWS config
aws sts get-caller-identity
```

### S3 Permission Denied

Ensure your AWS credentials have S3 permissions:

- `s3:PutObject` on target bucket
- `s3:GetObject` if uploading manifests
- `s3:ListBucket` for the bucket

### Out of GitHub API Rate Limit

Set `GITHUB_TOKEN` to a personal access token with higher rate limits (5000 requests/hour vs 60 unauthenticated).
