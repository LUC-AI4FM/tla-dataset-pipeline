from __future__ import annotations

from typing import Any

from langchain_anthropic import ChatAnthropic  # type: ignore
from langchain_huggingface import ChatHuggingFace, HuggingFaceEndpoint  # type: ignore
from langchain_ollama import ChatOllama  # type: ignore
from langchain_openai import ChatOpenAI
from pydantic import SecretStr


def create_llm(model_name: str = "gpt-4", api_key: str | None = None, provider: str = "openai") -> Any:
    if provider == "openai":
        return _make_openai(model_name, api_key)
    if provider == "ollama":
        return _make_ollama(model_name)
    if provider == "huggingface":
        return _make_huggingface(model_name, api_key)
    if provider == "anthropic":
        return _make_anthropic(model_name, api_key)

    raise ValueError(
        f"Unknown provider '{provider}'. Supported: openai, ollama, huggingface, anthropic"
    )


def _make_openai(model_name: str, api_key: str | None) -> Any:
    api_key_secret: SecretStr | None = SecretStr(api_key) if api_key is not None else None
    return ChatOpenAI(model=model_name, api_key=api_key_secret, temperature=0)


def _make_ollama(model_name: str) -> Any:
    return ChatOllama(model=model_name, temperature=0)


def _make_anthropic(model_name: str, api_key: str | None) -> Any:
    return ChatAnthropic(model=model_name, api_key=api_key, temperature=0)


def _make_huggingface(model_name: str, api_key: str | None) -> Any:
    endpoint = HuggingFaceEndpoint(
        repo_id=model_name,
        huggingfacehub_api_token=api_key,
        temperature=0,
    )
    return ChatHuggingFace(llm=endpoint)
