"""Human-approved schema-drift repair recipes.

Not a second workflow engine. This module is a small deterministic store of
HUMAN-APPROVED knowledge plus trusted code that executes one closed
transform: RENAME_COLUMN.

The LLM does not approve, invent, select, or execute repairs.
Recipe JSON describes the allowed operation; this file executes it.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline_reliability.adapters import RepairResult
from pipeline_reliability.state import PipelineReliabilityState

RENAME_COLUMN = "RENAME_COLUMN"
RENAMED_COLUMN = "RENAMED_COLUMN"
APPROVED = "APPROVED"
DEFAULT_RECIPE_DIR = Path(".local/repair_recipes/schema_drift")

_recipe_dir_override: Path | None = None
_source_path_override: Path | None = None
_staging_path_override: Path | None = None


@dataclass(frozen=True)
class RepairRecipe:
    """One human-approved repair. Not Agent State and not an Action."""

    recipe_id: str
    pipeline: str
    drift_type: str
    expected_schema: str
    observed_schema: str
    transform: dict[str, str]
    status: str
    approved_by: str
    approved_at: str
    signature: str


@dataclass(frozen=True)
class RepairSafety:
    """Independent APPLY_APPROVED_REPAIR authorization. Fail-closed."""

    allowed: bool
    reason: str
    recipe: RepairRecipe | None = None


class RepairError(ValueError):
    """Closed-set repair failed. Not a license to invent another transform."""


def configure_repair_io(
    *,
    recipe_dir: Path | None = None,
    source_path: Path | None = None,
    staging_path: Path | None = None,
) -> None:
    """Test/script override for the local recipe store and CSV paths."""
    global _recipe_dir_override, _source_path_override, _staging_path_override
    if recipe_dir is not None:
        _recipe_dir_override = Path(recipe_dir)
    if source_path is not None:
        _source_path_override = Path(source_path)
    if staging_path is not None:
        _staging_path_override = Path(staging_path)


def reset_repair_io() -> None:
    """Clear test/script path overrides."""
    global _recipe_dir_override, _source_path_override, _staging_path_override
    _recipe_dir_override = None
    _source_path_override = None
    _staging_path_override = None


def recipe_store_dir() -> Path:
    env = os.environ.get("PRA_REPAIR_RECIPES_DIR", "").strip()
    if env:
        return Path(env)
    if _recipe_dir_override is not None:
        return _recipe_dir_override
    return DEFAULT_RECIPE_DIR


def canonical_transform(transform: dict[str, Any]) -> dict[str, str]:
    """Normalize the closed transform object. Unknown keys are dropped."""
    return {
        "type": str(transform.get("type") or "").strip(),
        "from": str(transform.get("from") or "").strip(),
        "to": str(transform.get("to") or "").strip(),
    }


def signature_payload(
    *,
    pipeline: str,
    drift_type: str,
    expected_schema: str,
    observed_schema: str,
    transform: dict[str, Any],
) -> dict[str, Any]:
    """Fields bound into the signature. Extra recipe metadata is excluded."""
    return {
        "pipeline": pipeline,
        "drift_type": drift_type,
        "expected_schema": expected_schema,
        "observed_schema": observed_schema,
        "transform": canonical_transform(transform),
    }


def compute_signature(
    *,
    pipeline: str,
    drift_type: str,
    expected_schema: str,
    observed_schema: str,
    transform: dict[str, Any],
) -> str:
    """Stable SHA-256 of the exact-match identity. No fuzzy fields."""
    payload = signature_payload(
        pipeline=pipeline,
        drift_type=drift_type,
        expected_schema=expected_schema,
        observed_schema=observed_schema,
        transform=transform,
    )
    blob = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def implied_rename_transform(state: PipelineReliabilityState) -> dict[str, str] | None:
    """Closed-set transform for RENAMED_COLUMN. None if facts are incomplete."""
    if state.drift_type != RENAMED_COLUMN:
        return None
    changed = (state.changed_fields or "").strip()
    if "->" not in changed:
        return None
    expected_col, observed_col = changed.split("->", 1)
    expected_col = expected_col.strip()
    observed_col = observed_col.strip()
    if not expected_col or not observed_col:
        return None
    return {"type": RENAME_COLUMN, "from": observed_col, "to": expected_col}


def recipe_from_dict(payload: dict[str, Any]) -> RepairRecipe:
    transform = canonical_transform(payload.get("transform") or {})
    return RepairRecipe(
        recipe_id=str(payload.get("recipe_id") or ""),
        pipeline=str(payload.get("pipeline") or ""),
        drift_type=str(payload.get("drift_type") or ""),
        expected_schema=str(payload.get("expected_schema") or ""),
        observed_schema=str(payload.get("observed_schema") or ""),
        transform=transform,
        status=str(payload.get("status") or ""),
        approved_by=str(payload.get("approved_by") or ""),
        approved_at=str(payload.get("approved_at") or ""),
        signature=str(payload.get("signature") or ""),
    )


def recipe_to_dict(recipe: RepairRecipe) -> dict[str, Any]:
    return {
        "recipe_id": recipe.recipe_id,
        "pipeline": recipe.pipeline,
        "drift_type": recipe.drift_type,
        "expected_schema": recipe.expected_schema,
        "observed_schema": recipe.observed_schema,
        "transform": dict(recipe.transform),
        "status": recipe.status,
        "approved_by": recipe.approved_by,
        "approved_at": recipe.approved_at,
        "signature": recipe.signature,
    }


def load_recipe(path: Path) -> RepairRecipe:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RepairError("recipe file is not a JSON object")
    return recipe_from_dict(payload)


def save_recipe(recipe: RepairRecipe, directory: Path | None = None) -> Path:
    """Write `<signature>.json` atomically. Does not execute the transform."""
    target_dir = Path(directory) if directory is not None else recipe_store_dir()
    target_dir.mkdir(parents=True, exist_ok=True)
    path = target_dir / f"{recipe.signature}.json"
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(
        json.dumps(recipe_to_dict(recipe), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    tmp.replace(path)
    return path


def approve_schema_drift_recipe(
    *,
    recipe_id: str,
    pipeline: str,
    drift_type: str,
    expected_schema: str,
    observed_schema: str,
    transform: dict[str, str],
    approved_by: str,
    approved_at: str | None = None,
    status: str = APPROVED,
    directory: Path | None = None,
) -> RepairRecipe:
    """HUMAN-APPROVAL helper. Writes one recipe; the Agent does not call this.

    Callers supply pipeline/schema/transform identity. This function does not
    invent demo column names.
    """
    resolved_transform = canonical_transform(transform)
    signature = compute_signature(
        pipeline=pipeline,
        drift_type=drift_type,
        expected_schema=expected_schema,
        observed_schema=observed_schema,
        transform=resolved_transform,
    )
    recipe = RepairRecipe(
        recipe_id=recipe_id,
        pipeline=pipeline,
        drift_type=drift_type,
        expected_schema=expected_schema,
        observed_schema=observed_schema,
        transform=resolved_transform,
        status=status,
        approved_by=approved_by,
        approved_at=approved_at
        or datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        signature=signature,
    )
    save_recipe(recipe, directory)
    return recipe


def lookup_schema_drift_recipe(
    state: PipelineReliabilityState,
    directory: Path | None = None,
) -> RepairRecipe | None:
    """Load the recipe whose signature matches this incident. None if absent."""
    transform = implied_rename_transform(state)
    if transform is None:
        return None
    if not (
        state.pipeline
        and state.drift_type
        and state.expected_schema
        and state.observed_schema
    ):
        return None
    signature = compute_signature(
        pipeline=state.pipeline,
        drift_type=state.drift_type,
        expected_schema=state.expected_schema,
        observed_schema=state.observed_schema,
        transform=transform,
    )
    path = (directory or recipe_store_dir()) / f"{signature}.json"
    if not path.is_file():
        return None
    try:
        return load_recipe(path)
    except (OSError, json.JSONDecodeError, RepairError):
        return None


def exact_recipe_match(
    state: PipelineReliabilityState, recipe: RepairRecipe
) -> RepairSafety:
    """Exact field match. No fuzzy, Levenshtein, or LLM similarity."""
    transform = implied_rename_transform(state)
    expected_signature = ""
    if (
        state.pipeline
        and state.drift_type
        and state.expected_schema
        and state.observed_schema
        and transform is not None
    ):
        expected_signature = compute_signature(
            pipeline=state.pipeline,
            drift_type=state.drift_type,
            expected_schema=state.expected_schema,
            observed_schema=state.observed_schema,
            transform=transform,
        )
    checks = (
        ("pipeline", state.pipeline, recipe.pipeline),
        ("drift_type", state.drift_type, recipe.drift_type),
        ("expected_schema", state.expected_schema or "", recipe.expected_schema),
        ("observed_schema", state.observed_schema or "", recipe.observed_schema),
        ("transform", transform, recipe.transform),
        ("status", APPROVED, recipe.status),
        ("signature", expected_signature, recipe.signature),
    )
    for name, left, right in checks:
        if left != right:
            return RepairSafety(
                allowed=False,
                reason=f"APPLY_APPROVED_REPAIR rejected: {name} exact match failed.",
                recipe=recipe,
            )
    if recipe.transform.get("type") != RENAME_COLUMN:
        return RepairSafety(
            allowed=False,
            reason="APPLY_APPROVED_REPAIR rejected: transform type is not RENAME_COLUMN.",
            recipe=recipe,
        )
    return RepairSafety(
        allowed=True,
        reason="APPLY_APPROVED_REPAIR allowed: human-approved recipe exact match.",
        recipe=recipe,
    )


def evaluate_repair_safety(state: PipelineReliabilityState) -> RepairSafety:
    """Re-read the store and prove an exact approved match. Fail-closed."""
    if state.repair_applied is True:
        return RepairSafety(
            allowed=False,
            reason="APPLY_APPROVED_REPAIR rejected: repair already applied.",
        )
    if not state.expected_schema or not state.observed_schema or not state.drift_type:
        return RepairSafety(
            allowed=False,
            reason="APPLY_APPROVED_REPAIR rejected: schema drift facts are incomplete.",
        )
    recipe = lookup_schema_drift_recipe(state)
    if recipe is None:
        return RepairSafety(
            allowed=False,
            reason="APPLY_APPROVED_REPAIR rejected: no approved recipe for this signature.",
        )
    return exact_recipe_match(state, recipe)


def host_path_for(raw: str | Path) -> Path:
    """Translate a container `/opt/mdp/...` path to the host lab mount when needed."""
    path = Path(raw)
    root = (
        os.environ.get("MDP_PROJECT_ROOT", "").strip()
        or os.environ.get("MDP_HOST_ROOT", "").strip()
    )
    if root and (str(path).startswith("/opt/mdp/") or str(path) == "/opt/mdp"):
        return Path(root) / path.relative_to("/opt/mdp")
    return path


def resolve_source_csv(state: PipelineReliabilityState) -> Path:
    if _source_path_override is not None:
        return _source_path_override
    env = os.environ.get("PRA_SCHEMA_DRIFT_SOURCE", "").strip()
    if env:
        return Path(env)
    if (state.expected_object or "").strip():
        return host_path_for(state.expected_object)
    raise RepairError("incoming CSV path is unknown")


def resolve_staging_csv(state: PipelineReliabilityState) -> Path:
    if _staging_path_override is not None:
        return _staging_path_override
    env = os.environ.get("PRA_SCHEMA_DRIFT_STAGING", "").strip()
    if env:
        return Path(env)
    source = resolve_source_csv(state)
    return source.parent / "staging" / "orders_repaired.csv"


def read_csv_header(path: Path) -> tuple[str, ...]:
    with Path(path).open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        try:
            row = next(reader)
        except StopIteration as exc:
            raise RepairError(f"CSV has no header: {path}") from exc
    return tuple(cell.strip() for cell in row)


def validate_repaired_schema(
    header: tuple[str, ...] | list[str], expected_schema: str
) -> bool:
    """True only when the repaired header exactly equals the expected contract."""
    observed = ",".join(cell.strip() for cell in header)
    return observed == expected_schema


def rename_column_csv(
    source: Path,
    dest: Path,
    *,
    from_name: str,
    to_name: str,
) -> tuple[str, ...]:
    """Deterministic header rename. Preserves data rows. No eval()."""
    if from_name == to_name:
        raise RepairError("RENAME_COLUMN from and to must differ")
    with Path(source).open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if not rows:
        raise RepairError(f"CSV is empty: {source}")
    header = [cell.strip() for cell in rows[0]]
    if from_name not in header:
        raise RepairError(f"column {from_name!r} is not in the incoming header")
    if to_name in header:
        raise RepairError(f"column {to_name!r} already exists in the incoming header")
    repaired = [to_name if cell == from_name else cell for cell in header]
    dest = Path(dest)
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(repaired)
        writer.writerows(rows[1:])
    return tuple(repaired)


def apply_approved_repair(state: PipelineReliabilityState) -> RepairResult:
    """Execute the closed RENAME_COLUMN only after a fresh exact-match check.

    Writes a temp file, validates the repaired header, then publishes to
    staging. A validation miss never becomes the Airflow input.
    """
    safety = evaluate_repair_safety(state)
    recipe = safety.recipe
    if not safety.allowed or recipe is None:
        return RepairResult(
            applied=False,
            validated=False,
            recipe_id=recipe.recipe_id if recipe is not None else "",
            detail=safety.reason,
        )
    try:
        source = resolve_source_csv(state)
        staging = resolve_staging_csv(state)
    except RepairError as exc:
        return RepairResult(
            applied=False,
            validated=False,
            recipe_id=recipe.recipe_id,
            detail=f"APPLY_APPROVED_REPAIR rejected: {exc}",
        )
    tmp = staging.with_name(staging.name + ".tmp")
    try:
        repaired = rename_column_csv(
            source,
            tmp,
            from_name=recipe.transform["from"],
            to_name=recipe.transform["to"],
        )
        if not validate_repaired_schema(repaired, recipe.expected_schema):
            if tmp.exists():
                tmp.unlink()
            return RepairResult(
                applied=False,
                validated=False,
                recipe_id=recipe.recipe_id,
                detail=(
                    "APPLY_APPROVED_REPAIR rejected: repaired schema "
                    f"{','.join(repaired)} != {recipe.expected_schema}"
                ),
                repaired_schema=",".join(repaired),
                source_path=str(source),
                staging_path=str(staging),
            )
        tmp.replace(staging)
    except Exception as exc:
        if tmp.exists():
            tmp.unlink()
        return RepairResult(
            applied=False,
            validated=False,
            recipe_id=recipe.recipe_id,
            detail=f"APPLY_APPROVED_REPAIR rejected: {exc}",
            source_path=str(source),
            staging_path=str(staging),
        )
    return RepairResult(
        applied=True,
        validated=True,
        recipe_id=recipe.recipe_id,
        detail=(
            f"APPLY_APPROVED_REPAIR published {recipe.transform['from']} -> "
            f"{recipe.transform['to']} to {staging}"
        ),
        repaired_schema=",".join(repaired),
        source_path=str(source),
        staging_path=str(staging),
    )
