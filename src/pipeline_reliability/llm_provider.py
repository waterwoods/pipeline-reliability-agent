"""Optional fact-intelligence factory.

LLM_PROVIDER selects a FactIntelligence implementation. The default is none,
which keeps Decide → Guard → Execute → Apply fully deterministic.

This module does not read AWS access keys, secret keys, or session tokens.
Bedrock uses the boto3 default credential chain only when a client is created.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from typing import Callable

from pipeline_reliability.bedrock_intelligence import BedrockFactIntelligence
from pipeline_reliability.incident_analysis import DEFAULT_OPENAI_MODEL
from pipeline_reliability.intelligence import FactIntelligence, OpenAIFactIntelligence

_NONE = frozenset({"", "none"})


class LlmProviderConfigError(Exception):
    """LLM_PROVIDER is set, but the selected provider is not configured."""


def fact_intelligence_from_environ(
    environ: Mapping[str, str] | None = None,
    *,
    bedrock_client: object | None = None,
    bedrock_client_factory: Callable[[str], object] | None = None,
    openai_client: object | None = None,
    openai_client_factory: Callable[[], object] | None = None,
) -> FactIntelligence | None:
    """Return injected fact intelligence, or None when the provider is none.

    ``bedrock_client`` / ``openai_client`` exist so tests never open a network
    client. Production callers omit them and the factory builds the SDK client.
    """
    env = os.environ if environ is None else environ
    provider = (env.get("LLM_PROVIDER") or "none").strip().lower()
    if provider in _NONE:
        return None
    if provider == "openai":
        return _openai_intelligence(
            env,
            client=openai_client,
            client_factory=openai_client_factory,
        )
    if provider == "bedrock":
        return _bedrock_intelligence(
            env,
            client=bedrock_client,
            client_factory=bedrock_client_factory,
        )
    raise LlmProviderConfigError(
        "LLM_PROVIDER must be none, openai, or bedrock."
    )


def _openai_intelligence(
    env: Mapping[str, str],
    *,
    client: object | None,
    client_factory: Callable[[], object] | None,
) -> OpenAIFactIntelligence:
    model = (env.get("OPENAI_MODEL") or DEFAULT_OPENAI_MODEL).strip() or DEFAULT_OPENAI_MODEL
    if client is None:
        if client_factory is not None:
            client = client_factory()
        else:
            try:
                from openai import OpenAI
            except ImportError as exc:
                raise LlmProviderConfigError(
                    "LLM_PROVIDER=openai requires the llm extra."
                ) from exc
            client = OpenAI()
    return OpenAIFactIntelligence(client=client, model=model)


def _bedrock_intelligence(
    env: Mapping[str, str],
    *,
    client: object | None,
    client_factory: Callable[[str], object] | None,
) -> BedrockFactIntelligence:
    model_id = (env.get("BEDROCK_MODEL_ID") or "").strip()
    if not model_id:
        raise LlmProviderConfigError(
            "BEDROCK_MODEL_ID is required when LLM_PROVIDER=bedrock."
        )
    region = (env.get("BEDROCK_REGION") or env.get("AWS_REGION") or "").strip()
    if not region:
        raise LlmProviderConfigError(
            "AWS_REGION or BEDROCK_REGION is required when LLM_PROVIDER=bedrock."
        )
    if client is None:
        if client_factory is not None:
            client = client_factory(region)
        else:
            client = _default_bedrock_client(region)
    return BedrockFactIntelligence(client=client, model=model_id)


def _default_bedrock_client(region: str) -> object:
    """bedrock-runtime client. Credentials stay in the boto3 default chain."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as exc:
        raise LlmProviderConfigError(
            "LLM_PROVIDER=bedrock requires the bedrock extra."
        ) from exc
    return boto3.client(
        "bedrock-runtime",
        region_name=region,
        config=Config(
            connect_timeout=3,
            read_timeout=20,
            retries={"max_attempts": 1},
        ),
    )
