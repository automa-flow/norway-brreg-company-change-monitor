"""Store metadata must describe the runtime that actually ships."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from norway_brreg_company_change_monitor.billing import COMPANY_MONITORED_PRICE_USD
from norway_brreg_company_change_monitor.models import (
    ACTOR_NAME,
    EVENT_COMPANY_MONITORED,
    MAX_ORGANIZATION_NUMBERS,
    MAX_ORGNR_PER_CHUNK,
    SOURCE_NAME,
    ActorInput,
    BaselineMode,
    Mode,
    OutputStatus,
    RecordType,
    SourceChangeType,
    validate_organization,
)
from norway_brreg_company_change_monitor.monitor import ALL_CHANGE_TYPES
from norway_brreg_company_change_monitor.normalize import SEMANTIC_FIELDS
from norway_brreg_company_change_monitor.service import invalid_organization_record
from norway_brreg_company_change_monitor.state import KEY_PREFIX

ROOT = Path(__file__).parents[1]
ACTOR = ROOT / ".actor"


def read(name: str) -> dict[str, Any]:
    return json.loads((ACTOR / name).read_text(encoding="utf-8"))


def example(name: str) -> Any:
    return json.loads((ROOT / "examples" / name).read_text(encoding="utf-8"))


def readme() -> str:
    # Follow actor.json, so the same test holds in the monorepo and in the
    # standalone public export, where the Store listing lives in .actor/.
    return (ACTOR / read("actor.json")["readme"]).read_text(encoding="utf-8")


class TestIdentity:
    def test_the_actor_declares_a_build_context_that_carries_common(self) -> None:
        actor = read("actor.json")
        dockerfile = (ACTOR / actor["dockerfile"]).read_text(encoding="utf-8")

        assert actor["name"] == ACTOR_NAME
        assert actor["title"] == "Norway Company Registry Change Monitor - BRREG"
        assert len(actor["title"]) <= 60
        assert len(actor["description"]) <= 500
        # The source client builds on common/, so the build context must contain
        # it and the image must copy both it and this Actor's src/.
        context = (ACTOR / actor["dockerContextDir"]).resolve()
        source = (ROOT / "src").resolve().relative_to(context).as_posix()
        assert (context / "common").is_dir()
        assert "COPY common/ ./common/" in dockerfile
        assert f"COPY {source}/ ./src/" in dockerfile
        assert readme().startswith(f"# {actor['title']}\n")

    def test_the_store_listing_repeats_no_second_source_of_truth(self) -> None:
        actor = read("actor.json")
        listing = example("store-listing-metadata.json")

        assert listing["title"] == actor["title"]
        assert listing["description"] == actor["description"]
        assert listing["categories"] == actor["categories"]
        assert len(listing["seoTitle"]) <= 60
        assert len(listing["seoDescription"]) <= 160
        assert listing["pricingModel"] == "PAY_PER_EVENT"

    def test_the_search_intent_is_in_the_title_and_the_description(self) -> None:
        actor = read("actor.json")
        surface = f"{actor['title']} {actor['description']}".lower()
        for term in ("norway", "brreg", "company", "registry", "monitor", "change"):
            assert term in surface


class TestInputContract:
    def test_the_public_input_exposes_no_source_internals(self) -> None:
        properties = read("input_schema.json")["properties"]

        assert set(properties) == {
            "organizationNumbers",
            "monitorKey",
            "mode",
            "baselineMode",
            "includeSourcePatch",
        }
        assert properties["organizationNumbers"]["minItems"] == 1
        assert properties["organizationNumbers"]["maxItems"] == MAX_ORGANIZATION_NUMBERS
        # Chunk width, cursor, concurrency and retries are ours to get right
        # against a free public register, not knobs a customer can turn.
        assert {
            "concurrency",
            "chunkSize",
            "pageSize",
            "cursor",
            "retries",
            "proxyConfiguration",
        }.isdisjoint(properties)

    def test_the_runtime_accepts_exactly_what_the_schema_advertises(self) -> None:
        schema = read("input_schema.json")["properties"]
        parsed = ActorInput.model_validate(
            {
                "organizationNumbers": schema["organizationNumbers"]["prefill"],
                "monitorKey": schema["monitorKey"]["prefill"],
                "mode": schema["mode"]["default"],
                "baselineMode": schema["baselineMode"]["default"],
                "includeSourcePatch": schema["includeSourcePatch"]["default"],
            }
        )
        assert parsed.mode is Mode.CHANGES_ONLY
        assert parsed.baseline_mode is BaselineMode.EMIT_SNAPSHOT
        assert list(schema["mode"]["enum"]) == [str(value) for value in Mode]
        assert list(schema["baselineMode"]["enum"]) == [str(value) for value in BaselineMode]
        assert len(schema["mode"]["enumTitles"]) == len(schema["mode"]["enum"])
        assert len(schema["baselineMode"]["enumTitles"]) == len(schema["baselineMode"]["enum"])

    def test_every_advertised_example_value_actually_validates(self) -> None:
        schema = read("input_schema.json")["properties"]["organizationNumbers"]
        for raw in [*schema["prefill"], *schema["example"]]:
            prepared = validate_organization(raw, index=0)
            assert getattr(prepared, "organization_number", None), raw

    def test_the_monitor_key_pattern_matches_the_state_namespace(self) -> None:
        schema = read("input_schema.json")["properties"]["monitorKey"]
        assert schema["pattern"] == "^[A-Za-z0-9_-]{1,80}$"
        assert KEY_PREFIX.startswith("BRREG_MONITOR_STATE_V")


class TestOutputContract:
    def test_the_dataset_schema_declares_every_field_the_runtime_emits(self) -> None:
        declared = read("dataset_schema.json")["fields"]["properties"]
        rows = example("sample_output.json")
        assert rows
        for row in rows:
            assert set(row) == set(declared), set(row).symmetric_difference(declared)
        invalid_row = invalid_organization_record(
            type("I", (), {"index": 0, "submitted": "x", "code": "C", "message": "m"})(),
            observed_at="2026-09-08T00:00:00Z",
            monitor_key="k",
        )
        assert set(invalid_row) == set(declared)

    def test_the_declared_vocabularies_match_the_runtime_enums(self) -> None:
        declared = read("dataset_schema.json")["fields"]["properties"]
        assert set(declared["record_type"]["enum"]) == {str(value) for value in RecordType}
        assert set(declared["status"]["enum"]) == {str(value) for value in OutputStatus}
        assert set(declared["change_types"]["items"]["enum"]) == set(ALL_CHANGE_TYPES)
        assert set(declared["patch_change_types"]["items"]["enum"]) == set(ALL_CHANGE_TYPES)
        assert set(declared["source_change_types"]["items"]["enum"]) == {
            str(value) for value in SourceChangeType
        }

    def test_the_current_block_declares_the_whole_normalized_snapshot(self) -> None:
        declared = read("dataset_schema.json")["fields"]["properties"]["current"]["properties"]
        assert set(SEMANTIC_FIELDS) <= set(declared)
        # Nothing personal may be declared, let alone emitted.
        assert {"epostadresse", "mobil", "telefon", "email", "phone", "roles"}.isdisjoint(declared)

    def test_no_row_or_example_carries_a_contact_detail(self) -> None:
        published = json.dumps(example("sample_output.json"), ensure_ascii=False)
        assert "@" not in published
        for row in example("sample_output.json"):
            assert "epostadresse" not in json.dumps(row.get("current") or {})
            for operation in row["source_patch"]:
                if operation["path"] in ("/epostadresse", "/mobil", "/telefon"):
                    assert operation.get("value_redacted") is True
                    assert "value" not in operation

    def test_every_dataset_view_projects_declared_fields(self) -> None:
        schema = read("dataset_schema.json")
        declared = set(schema["fields"]["properties"])
        assert set(schema["views"]) == {"changes", "current", "diagnostics", "monitoring"}
        for view in schema["views"].values():
            transformation = view["transformation"]
            assert set(transformation["fields"]) <= declared
            assert set(transformation.get("unwind", [])) <= declared
            assert set(view["display"]["properties"]) <= set(transformation["fields"])

    def test_the_output_schema_points_at_views_that_exist(self) -> None:
        views = set(read("dataset_schema.json")["views"])
        for entry in read("output_schema.json")["properties"].values():
            template = entry["template"]
            if "view=" in template:
                assert template.split("view=")[1].split("&")[0] in views

    def test_the_run_summary_schema_matches_what_the_runtime_writes(self) -> None:
        required = set(
            read("key_value_store_schema.json")["collections"]["runSummary"]["jsonSchema"][
                "required"
            ]
        )
        declared = set(
            read("key_value_store_schema.json")["collections"]["runSummary"]["jsonSchema"][
                "properties"
            ]
        )
        assert required <= declared
        assert {"cursor_before", "cursor_after", "cursor_advanced"} <= required

    def test_every_row_names_its_source_and_its_own_past(self) -> None:
        for row in example("sample_output.json"):
            assert row["source"] == SOURCE_NAME
            assert row["source_id"]
            assert row["scraped_at"].endswith("Z")
            assert row["schema_version"] >= 1


class TestCommercialContract:
    def test_one_paid_event_priced_exactly_as_the_runtime_expects(self) -> None:
        events = json.loads((ACTOR / "pay_per_event.json").read_text(encoding="utf-8"))
        assert set(events) == {EVENT_COMPANY_MONITORED, "apify-actor-start"}
        event = events[EVENT_COMPANY_MONITORED]
        assert event["eventTieredPricingUsd"]["FREE"]["tieredEventPriceUsd"] == float(
            COMPANY_MONITORED_PRICE_USD
        )
        assert events["apify-actor-start"]["eventPriceUsd"] == 0.005
        assert read("actor.json")["maxMemoryMbytes"] <= 1024
        assert event["isPrimaryEvent"] is True
        assert event["isOneTimeEvent"] is False

    def test_the_event_description_states_what_is_never_charged(self) -> None:
        events = json.loads((ACTOR / "pay_per_event.json").read_text(encoding="utf-8"))
        description = events[EVENT_COMPANY_MONITORED]["eventDescription"].lower()
        for promise in ("invalid", "retries", "replayed", "charge limit", "recovery"):
            assert promise in description

    def test_the_readme_quotes_the_price_the_code_charges(self) -> None:
        text = readme()
        assert f"${float(COMPANY_MONITORED_PRICE_USD):.4f}" in text
        assert "$0.10" in text and "$0.50" in text and "$2.50" in text
        assert "Never charged" in text

    def test_the_sample_input_is_bounded_and_cheap(self) -> None:
        sample = example("sample_input.json")
        assert len(sample["organizationNumbers"]) <= 5
        assert ActorInput.model_validate(sample)


class TestDocumentedBehaviour:
    def test_the_readme_documents_every_change_type_the_code_can_emit(self) -> None:
        text = readme()
        for change_type in ALL_CHANGE_TYPES:
            assert change_type in text, change_type
        for source_type in SourceChangeType:
            assert str(source_type) in text

    def test_the_readme_states_the_failure_semantics_it_promises(self) -> None:
        text = readme()
        for state in ("NOT_FOUND", "REMOVED", "SOURCE_FAILED", "PARTIAL", "INVALID_INPUT"):
            assert state in text
        assert "410" in text
        assert "cursor" in text

    def test_the_readme_states_the_privacy_scope_the_code_enforces(self) -> None:
        text = readme().lower()
        for promise in ("no people", "no contact details", "no street lines", "nlod"):
            assert promise in text

    def test_the_readme_quotes_the_measured_chunk_width(self) -> None:
        assert f"chunked at {MAX_ORGNR_PER_CHUNK} organization numbers" in readme()

    def test_the_readme_shows_a_schedule_and_a_webhook(self) -> None:
        text = readme()
        assert "ACTOR.RUN.SUCCEEDED" in text
        assert "0 6 * * *" in text
        assert "run-sync-get-dataset-items" in text
