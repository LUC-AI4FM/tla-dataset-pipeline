"""Orchestration layer for discovery operations."""

from typing import Any

from tladata.contracts.validate import validate_jsonl
from tladata.discovery.github_client import GithubClient
from tladata.discovery.github_search import (
    fetch_repo_metadata,
    search_repositories,
)
from tladata.discovery.manifest import merge_records, write_jsonl
from tladata.utils.load_seeds import load_queries, load_seed_repos


class DiscoveryPipeline:
    """Full discovery pipeline: seeds + search + write + validate."""

    def __init__(self, client: GithubClient, output_path: str, schema_path: str):
        self.client = client
        self.output_path = output_path
        self.schema_path = schema_path

    def run(self) -> None:
        """Execute the full discovery pipeline."""
        seeds = load_seed_repos()
        queries = load_queries()

        discovered = self._fetch_seeds(seeds)
        discovered = self._run_searches(queries, discovered)

        write_jsonl(self.output_path, discovered.values())
        print(f"Manifest written to: {self.output_path}")

        self._validate()

    def _fetch_seeds(self, seeds: list[str]) -> dict[str, dict[str, Any]]:
        """Fetch metadata for seeded repositories."""
        discovered: dict[str, dict[str, Any]] = {}
        for repo in seeds:
            discovered[repo] = fetch_repo_metadata(self.client, repo, source="seed")
        return discovered

    def _run_searches(
        self, queries: list[str], discovered: dict[str, dict[str, Any]]
    ) -> dict[str, dict[str, Any]]:
        """Run search queries and merge results."""
        for query in queries:
            repos = search_repositories(self.client, query)
            for repo_dict in repos:
                repo_name = str(repo_dict.get("full_name", ""))
                new_metadata = fetch_repo_metadata(self.client, repo_name, source=f"query:{query}")
                if repo_name in discovered:
                    discovered[repo_name] = merge_records(discovered[repo_name], new_metadata)
                else:
                    discovered[repo_name] = new_metadata
        return discovered

    def _validate(self) -> None:
        """Validate the output manifest."""
        success, errors = validate_jsonl(self.output_path, self.schema_path)
        if success:
            print(f"Validation passed against: {self.schema_path}")
        else:
            raise RuntimeError("Validation failed:\n" + "\n".join(errors))


class SearchService:
    """Search-only service."""

    def __init__(self, client: GithubClient, output_path: str):
        self.client = client
        self.output_path = output_path

    def run(self) -> None:
        """Execute search queries and write results."""
        queries = load_queries()
        discovered: dict[str, dict[str, Any]] = {}

        for query in queries:
            repos = search_repositories(self.client, query)
            for repo_dict in repos:
                repo_name = str(repo_dict.get("full_name", ""))
                new_metadata = fetch_repo_metadata(self.client, repo_name, source=f"query:{query}")
                if repo_name in discovered:
                    discovered[repo_name] = merge_records(discovered[repo_name], new_metadata)
                else:
                    discovered[repo_name] = new_metadata

        write_jsonl(self.output_path, discovered.values())
        print(f"Search results written to: {self.output_path}")


class SeedFetcher:
    """Fetches metadata for seeded repositories only."""

    def __init__(self, client: GithubClient, output_path: str):
        self.client = client
        self.output_path = output_path

    def run(self) -> None:
        """Fetch seeded repos and write results."""
        seeds = load_seed_repos()
        discovered: dict[str, dict[str, Any]] = {}

        for repo in seeds:
            discovered[repo] = fetch_repo_metadata(self.client, repo, source="seed")

        write_jsonl(self.output_path, discovered.values())
        print(f"Seed repos metadata written to: {self.output_path}")


class ManifestValidator:
    """Validates manifests against schemas."""

    def __init__(self, manifest_path: str, schema_path: str):
        self.manifest_path = manifest_path
        self.schema_path = schema_path

    def validate(self) -> None:
        """Validate the manifest."""
        success, errors = validate_jsonl(self.manifest_path, self.schema_path)
        if success:
            print(f"Validation passed: {self.manifest_path} against {self.schema_path}")
        else:
            raise RuntimeError("Validation failed:\n" + "\n".join(errors))
