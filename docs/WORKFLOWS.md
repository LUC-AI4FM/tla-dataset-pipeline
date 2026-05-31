# TLA Dataset Pipeline - GitHub Workflows

## Quick Start

1. Go to your repository on GitHub
2. Click **Actions** tab
3. Click **Run Full Pipeline** workflow
4. Click **Run workflow** button
5. Configure your experiment (see below)
6. Click **Run workflow** to start

## The Workflow

This workflow chains together the entire pipeline:

- **Discovery** - Find repositories on GitHub
- **Extraction** - Extract `.tla`, `.cfg`, `.tlaps` files
- **Parsing** (optional) - Analyze with LLMs
- **S3 Upload** (optional) - Archive to AWS
- **Auto-commit** - Save results to repository

## Common Usage Patterns

All experiments use **Run Full Pipeline** with different configurations:

### 1. Just Discovery (10 min)

```text
Discovery: search
Extraction: OFF
Parsing: OFF
Upload: OFF
Commit: ON
```

Results: `manifests/sources/sources_latest.jsonl`

### 2. Discovery + Extraction (20 min)

```text
Discovery: discover
Extraction: ON
Parsing: OFF
Upload: OFF
Commit: ON
```

Results: `data/raw/` with extracted files

### 3. Full Pipeline with Parsing (2 hours)

```text
Discovery: discover
Extraction: ON
Parsing: ON (model: gpt-4o)
Upload: OFF
Commit: ON
```

Results: `data/parsed/` with parsing results

### 4. Everything to S3 (3 hours)

```text
Discovery: discover
Extraction: ON
Parsing: ON (model: ollama:llama3)
Upload: ON
Commit: ON
```

Results: S3 bucket + local repo backup

## Required Setup (One-time)

### Secrets (Settings > Secrets and variables > Actions)

```text
*  GITHUB_TOKEN          (automatic)
*  AWS_ACCESS_KEY_ID     (for S3)
*  AWS_SECRET_ACCESS_KEY (for S3)
```

Optional (for parsing):

```text
○ OPENAI_API_KEY        (for GPT models)
○ ANTHROPIC_API_KEY     (for Claude)
○ HUGGINGFACE_API_KEY   (for open models)
```

### Variables (Settings > Secrets and variables > Variables)

```text
S3_BUCKET     (your S3 bucket name)
```

## Workflow Input Parameters

All configuration happens in the **Run Full Pipeline** workflow:

| Parameter | Default | Description |
|-----------|---------|-------------|
| discovery_command | discover | `discover` (full search), `fetch-seeds` (seeded repos only), or `search` (search queries) |
| run_extraction | true | Enable extraction of `.tla`, `.cfg`, `.tlaps` files |
| run_parsing | false | Enable LLM-based parsing of extracted files |
| parsing_model | gpt-4 | LLM model: `gpt-4`, `gpt-4o`, `ollama:llama3`, `anthropic:claude-3-sonnet-20240229`, etc. |
| run_s3_upload | false | Enable upload to S3 |
| s3_bucket | (from .dvc/config) | S3 bucket name (auto-detected if using .dvc/config) |
| s3_prefix | raw | S3 folder path |
| commit_results | true | Auto-commit results to repository |

## Viewing Results

### During Workflow

1. Go to Actions tab
2. Click running workflow
3. Expand steps to see logs in real-time

### After Workflow

1. Go to Actions > Workflow run
2. Scroll to "Artifacts" section
3. Download summary files or results
4. Check "Code" tab if results committed to repo

## Monitoring Queue

Check if workflows are queued:

1. Actions > All workflows
2. Look for "Queued" or "In progress" badges
3. Workflows run sequentially; be patient if many are queued

## Need Help?

1. **Check the logs** - Click workflow run > expand failed step > read error message
2. **Review README** - [README.md](../README.md) has detailed CLI documentation
3. **Ask the team** - GitHub Discussions or team channel

---

**For questions:** See repository documentation or open an issue
