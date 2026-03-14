from pathlib import Path


def load_env() -> None:
    try:
        from dotenv import load_dotenv

        env_path = Path.cwd() / ".env"
        if env_path.exists():
            load_dotenv(env_path)
    except ImportError:
        # python-dotenv not installed (CI/CD environment) - this is expected
        pass
