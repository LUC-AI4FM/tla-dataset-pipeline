"""Handler for TLA+ prompt-based parsing operations."""

import os
from pathlib import Path
from typing import TYPE_CHECKING

from tladata.cli_handler import CLIHandler
from tladata.logging import get_logger
from tladata.parsing.pipeline import PromptPipeline

if TYPE_CHECKING:
    import argparse

class ParsingHandler(CLIHandler):
    """Handle TLA+ prompt-based parsing operations.

    Provides CLI interface for running V1, V1+V2, or full V1+V2+V3 pipelines
    on TLA+ specification files.
    """

    def __init__(self) -> None:
        """Initialize the parsing handler."""
        super().__init__()
        self.logger = get_logger(self.__class__.__name__)

    def _get_api_key(self, provider: str) -> str | None:
        provider = provider.lower()
        if provider == "openai":
            token = os.environ.get("OPENAI_API_KEY")
            if not token:
                self.logger.error("OPENAI_API_KEY environment variable is not set")
                exit(1)
            return token
        if provider == "anthropic":
            token = os.environ.get("ANTHROPIC_API_KEY")
            if not token:
                self.logger.error("ANTHROPIC_API_KEY environment variable is not set")
                exit(1)
            return token
        if provider == "huggingface":
            token = os.environ.get("HF_TOKEN")
            if not token:
                self.logger.warning(
                    "HF_TOKEN not set; using unauthenticated requests which may have limited access or rate limits."
                )
            return token
        if provider == "ollama":
            return None
        return None

    def _sanitize_model_name(self, model_spec: str) -> str:
        """Sanitize model name for use as a directory name.
        Converts model specs like:
        - "openai:gpt-4o" -> "openai-gpt-4o"
        - "huggingface:mistralai/Mistral-7B" -> "huggingface-mistralai_Mistral-7B"

        Args:
            model_spec: Model specification string

        Returns:
            Sanitized directory name
        """
        # Replace colons with hyphens (for provider:model format)
        sanitized = model_spec.replace(":", "-")
        # Replace forward slashes with underscores (for paths like mistralai/Mistral)
        sanitized = sanitized.replace("/", "_")
        # Replace any other problematic characters with underscores
        sanitized = "".join(c if c.isalnum() or c in "-_" else "_" for c in sanitized)
        return sanitized

    def handle(self, args: "argparse.Namespace") -> int:
        """Execute prompt-based parsing pipeline.

        Args:
            args: Arguments containing:
                - input: Path to TLA+ file or directory
                - output: Directory for saving results
                - version: Pipeline version (1, 2, or 3)
                - skip_existing: Skip if results already exist

        Returns:
            Exit code (0 for success, 1 for failure)
        """
        try:
            model_spec = getattr(args, "model", "gpt-4")
            provider = getattr(args, "provider", None)
            if not provider:
                provider = model_spec.split(":", 1)[0] if ":" in model_spec else "openai"
            api_key = self._get_api_key(provider)

            input_path = Path(args.input)
            if not input_path.exists():
                self.logger.error(f"Input path does not exist: {args.input}")
                return 1

            # Construct output directory with model name
            model_name_clean = self._sanitize_model_name(model_spec)
            output_dir = Path(args.output) / model_name_clean

            self.logger.info(f"Using output directory: {output_dir}")

            pipeline = PromptPipeline(
                output_dir=str(output_dir),
                model_name=model_spec,
                provider=provider,
                api_key=api_key,
            )

            # Determine version and run appropriate pipeline
            version = getattr(args, "version", 3)
            skip_existing = getattr(args, "skip_existing", True)

            if input_path.is_file():
                # Process single file
                self._process_single_file(pipeline, str(input_path), version, skip_existing)
            elif input_path.is_dir():
                # Process directory of files
                self._process_directory(pipeline, str(input_path), version, skip_existing)
            else:
                self.logger.error(f"Invalid input path: {args.input}")
                return 1

            self.logger.info("Parsing completed successfully")
            return 0

        except Exception as e:
            self.logger.error(f"Parsing failed: {e}")
            return 1

    def _process_single_file(
        self,
        pipeline: PromptPipeline,
        file_path: str,
        version: int,
        skip_existing: bool,
    ) -> None:
        """Process a single TLA+ file.

        Args:
            pipeline: PromptPipeline instance
            file_path: Path to TLA+ file
            version: Pipeline version (1, 2, or 3)
            skip_existing: Skip if results already exist
        """
        filename = Path(file_path).name
        self.logger.info(f"Processing {filename}")

        try:
            if version == 1:
                pipeline.run_v1_only(file_path, skip_existing)
            elif version == 2:
                pipeline.run_v1_v2(file_path, skip_existing)
            elif version == 3:
                pipeline.run_full_pipeline(file_path, skip_existing)
            else:
                self.logger.error(f"Invalid version: {version}")
                return

            self.logger.info(f"Successfully processed {filename}")
        except Exception as e:
            self.logger.error(f"Failed to process {filename}: {e}")

    def _process_directory(
        self,
        pipeline: PromptPipeline,
        dir_path: str,
        version: int,
        skip_existing: bool,
    ) -> None:
        """Process all TLA+ files in a directory.

        Args:
            pipeline: PromptPipeline instance
            dir_path: Path to directory
            version: Pipeline version (1, 2, or 3)
            skip_existing: Skip if results already exist
        """
        tla_files = list(Path(dir_path).glob("**/*.tla"))
        if not tla_files:
            self.logger.warning(f"No TLA+ files found in {dir_path}")
            return

        self.logger.info(f"Found {len(tla_files)} TLA+ files to process")

        success_count = 0
        for tla_file in tla_files:
            try:
                self._process_single_file(pipeline, str(tla_file), version, skip_existing)
                success_count += 1
            except Exception as e:
                self.logger.warning(f"Skipping {tla_file.name}: {e}")
                continue

        self.logger.info(f"Processed {success_count}/{len(tla_files)} files successfully")
