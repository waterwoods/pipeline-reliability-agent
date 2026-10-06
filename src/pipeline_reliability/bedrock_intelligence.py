"""Bedrock-backed FactIntelligence. Proposes facts only; never an Action.

Inject a boto3 bedrock-runtime client. This class calls Converse and maps
the text to FactProposal. It does not construct clients, read credentials,
mutate State, call Decide/Guard/Execute, or send toolConfig.

Structured output asks the model for JSON. Acceptance still belongs to
validate_fact_proposal(). A schema match is not authorization.
"""

from __future__ import annotations

import json
from dataclasses import asdict

from pipeline_reliability.adapters import TaskLogResult
from pipeline_reliability.facts import FORBIDDEN_FIELDS, FactProposal
from pipeline_reliability.intelligence import (
    FACT_PROPOSAL_JSON_SCHEMA,
    _REPAIR_SYSTEM_PROMPT,
    _SYSTEM_PROMPT,
)

# Keys the model must not use to choose an action. Forbidden fact keys are
# copied onto facts so the existing validator rejects them. This class does
# not invent a second allow/deny policy.
_AUTHORITY_KEYS = FORBIDDEN_FIELDS | frozenset(
    {
        "BACKFILL",
        "BACKFILL_PARTITION",
        "EXECUTE",
        "RETRY",
        "authorization",
        "execute",
    }
)

_MAX_TOKENS = 800


def fact_proposal_output_config() -> dict[str, object]:
    """Converse outputConfig for the existing fact-proposal schema.

    The schema string is the same contract OpenAI structured output uses.
    Bedrock enforcing it does not replace validate_fact_proposal().
    """
    return {
        "textFormat": {
            "type": "json_schema",
            "structure": {
                "jsonSchema": {
                    "schema": json.dumps(
                        FACT_PROPOSAL_JSON_SCHEMA,
                        separators=(",", ":"),
                        sort_keys=True,
                    ),
                    "name": "fact_proposal",
                    "description": (
                        "Pipeline failure facts only. Never an action, "
                        "RETRY, BACKFILL, or authorization."
                    ),
                }
            },
        }
    }


def _observation_payload(observation: TaskLogResult) -> str:
    return (
        f"error_type: {observation.error_type}\n"
        f"message: {observation.message}\n"
        f"detail: {observation.detail}"
    )


def _text_from_converse(response: object) -> str:
    if not isinstance(response, dict):
        raise ValueError("Bedrock response is not a Converse payload.")
    try:
        content = response["output"]["message"]["content"]
    except (KeyError, TypeError) as exc:
        raise ValueError("Bedrock response is missing message content.") from exc
    if not isinstance(content, list):
        raise ValueError("Bedrock response content is not a list.")
    texts: list[str] = []
    for block in content:
        if not isinstance(block, dict):
            raise ValueError("Bedrock response content block is not an object.")
        if "toolUse" in block or "toolResult" in block:
            raise ValueError(
                "Bedrock response included tool use. "
                "Fact intelligence does not accept tool calls."
            )
        text = block.get("text")
        if isinstance(text, str):
            texts.append(text)
    combined = "".join(texts).strip()
    if not combined:
        raise ValueError("Bedrock response content is empty.")
    return combined


def _facts_for_validator(payload: dict[str, object]) -> dict[str, object]:
    raw_facts = payload.get("facts")
    if not isinstance(raw_facts, dict):
        raise ValueError("Bedrock fact response facts must be an object.")
    facts: dict[str, object] = {
        key: value for key, value in raw_facts.items() if value is not None
    }
    for key, value in payload.items():
        if key in {"failure_type", "facts", "confidence", "evidence"}:
            continue
        if key in _AUTHORITY_KEYS:
            facts[key] = value
    return facts


def _payload_to_proposal(payload: object) -> FactProposal:
    """Convert Converse JSON into FactProposal. Does not validate or write State."""
    if not isinstance(payload, dict):
        raise ValueError("Bedrock fact response must be a JSON object.")
    try:
        failure_type = payload["failure_type"]
        confidence = payload["confidence"]
        evidence = payload["evidence"]
    except KeyError as exc:
        raise ValueError(f"Bedrock fact response missing {exc.args[0]}.") from exc
    return FactProposal(
        failure_type=failure_type,  # type: ignore[arg-type]
        facts=_facts_for_validator(payload),
        confidence=confidence,  # type: ignore[arg-type]
        evidence=evidence,  # type: ignore[arg-type]
    )


class BedrockFactIntelligence:
    """Converse client in, FactProposal out. No tool use and no action authority.

    Optional demo::

        intelligence = BedrockFactIntelligence(client=client, model=model_id)
        run_agent(state, adapter=adapter, intelligence=intelligence)
    """

    provider = "bedrock"

    def __init__(self, client: object, model: str) -> None:
        if not model.strip():
            raise ValueError("Bedrock model id is required.")
        self._client = client
        self._model = model.strip()

    @property
    def model(self) -> str:
        return self._model

    def propose_facts(self, observation: TaskLogResult) -> FactProposal:
        return self._request_proposal(
            _SYSTEM_PROMPT,
            _observation_payload(observation),
        )

    def repair_facts(
        self,
        observation: TaskLogResult,
        rejected_proposal: FactProposal,
        rejection_reason: str,
    ) -> FactProposal:
        user_content = (
            f"{_observation_payload(observation)}\n\n"
            "Your previous FactProposal was rejected by the deterministic validator.\n\n"
            f"Reason:\n{rejection_reason}\n\n"
            "Previous FactProposal:\n"
            f"{json.dumps(asdict(rejected_proposal), indent=2)}\n\n"
            "Return a corrected FactProposal using ONLY evidence supported by the "
            "original log.\n"
            "Do NOT invent facts merely to satisfy the validator.\n"
            "If the evidence does not support the missing facts, return UNKNOWN or "
            "another evidence-supported proposal."
        )
        return self._request_proposal(_REPAIR_SYSTEM_PROMPT, user_content)

    def _request_proposal(self, system_prompt: str, user_text: str) -> FactProposal:
        try:
            response = self._client.converse(  # type: ignore[attr-defined]
                modelId=self._model,
                system=[{"text": system_prompt}],
                messages=[
                    {"role": "user", "content": [{"text": user_text}]},
                ],
                inferenceConfig={"maxTokens": _MAX_TOKENS, "temperature": 0},
                outputConfig=fact_proposal_output_config(),
            )
            payload = json.loads(_text_from_converse(response))
            return _payload_to_proposal(payload)
        except Exception as exc:
            raise ValueError(
                "Bedrock fact intelligence failed to produce a FactProposal."
            ) from exc
