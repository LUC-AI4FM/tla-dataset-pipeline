import yaml


def load_seed_repos() -> list[str]:
    with open("config/seeds/repos.yaml") as f:
        data = yaml.safe_load(f)
        return data if isinstance(data, list) else []


def load_queries() -> list[str]:
    with open("config/seeds/queries.yaml") as f:
        data = yaml.safe_load(f)
        return data if isinstance(data, list) else []
