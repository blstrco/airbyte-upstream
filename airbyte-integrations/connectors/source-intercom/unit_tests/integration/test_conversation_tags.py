# Copyright (c) 2026 Airbyte, Inc., all rights reserved.

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional
from unittest import TestCase

import requests_mock
from config_builder import ConfigBuilder

from airbyte_cdk import ConfiguredAirbyteCatalog, SyncMode, TState, YamlDeclarativeSource
from airbyte_cdk.test.catalog_builder import CatalogBuilder
from airbyte_cdk.test.entrypoint_wrapper import EntrypointOutput
from airbyte_cdk.test.entrypoint_wrapper import read as entrypoint_read
from airbyte_cdk.test.state_builder import StateBuilder


def _get_manifest_path() -> Path:
    source_declarative_manifest_path = Path("/airbyte/integration_code/source_declarative_manifest")
    if source_declarative_manifest_path.exists():
        return source_declarative_manifest_path
    return Path(__file__).parent.parent.parent


_SOURCE_FOLDER_PATH = _get_manifest_path()
_YAML_FILE_PATH = _SOURCE_FOLDER_PATH / "manifest.yaml"
_STREAM_NAME = "conversation_tags"
_NOW = datetime.now(timezone.utc)
_UPDATED_AT = int((_NOW - timedelta(minutes=10)).timestamp())
_EMPTY_TAGS = {"type": "tag.list", "tags": []}
_LIVE_TAGS = {
    "type": "tag.list",
    "tags": [
        {
            "type": "tag",
            "id": "0123",
            "name": "It's \"VIP\"",
            "applied_at": _UPDATED_AT,
            "applied_by": {"type": "admin", "id": "456"},
        }
    ],
}

sys.path.append(str(_SOURCE_FOLDER_PATH))  # to allow loading custom components


def _catalog() -> ConfiguredAirbyteCatalog:
    return CatalogBuilder().with_stream(_STREAM_NAME, SyncMode.incremental).build()


def _source(catalog: ConfiguredAirbyteCatalog, config: Mapping[str, Any], state: Optional[TState]) -> YamlDeclarativeSource:
    return YamlDeclarativeSource(path_to_yaml=str(_YAML_FILE_PATH), catalog=catalog, config=config, state=state)


def read(state_builder: Optional[StateBuilder] = None) -> EntrypointOutput:
    catalog = _catalog()
    config = ConfigBuilder().start_date(_NOW - timedelta(hours=1)).build()
    state = state_builder.build() if state_builder else StateBuilder().build()
    return entrypoint_read(_source(catalog, config, state), config, catalog, state)


def _search_response(conversation_ids: List[str]) -> Dict[str, Any]:
    return {
        "type": "conversation.list",
        "pages": {},
        "conversations": [
            {"type": "conversation", "id": conversation_id, "updated_at": _UPDATED_AT, "tags": _EMPTY_TAGS}
            for conversation_id in conversation_ids
        ],
    }


def _conversation(conversation_id: str, tags: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "conversation",
        "id": conversation_id,
        "created_at": _UPDATED_AT - 60,
        "updated_at": _UPDATED_AT,
        "state": "open",
        "tags": tags,
        "conversation_parts": {"type": "conversation_part.list", "conversation_parts": [], "total_count": 0},
    }


class ConversationTagsTest(TestCase):
    def test_emits_one_record_per_conversation_with_tags_from_the_conversation_itself(self) -> None:
        with requests_mock.Mocker() as http_mocker:
            http_mocker.post("https://api.intercom.io/conversations/search", json=_search_response(["0123", "124"]))
            tagged_conversation = http_mocker.get("https://api.intercom.io/conversations/0123", json=_conversation("0123", _LIVE_TAGS))
            untagged_conversation = http_mocker.get("https://api.intercom.io/conversations/124", json=_conversation("124", _EMPTY_TAGS))

            output = read()

        assert tagged_conversation.call_count == 1
        assert untagged_conversation.call_count == 1
        assert sorted((record.record.data for record in output.records), key=lambda data: data["id"]) == [
            {"id": "0123", "updated_at": _UPDATED_AT, "tags": _LIVE_TAGS},
            {"id": "124", "updated_at": _UPDATED_AT, "tags": _EMPTY_TAGS},
        ]

    def test_skips_conversation_that_is_not_found(self) -> None:
        with requests_mock.Mocker() as http_mocker:
            http_mocker.post("https://api.intercom.io/conversations/search", json=_search_response(["123", "124"]))
            http_mocker.get("https://api.intercom.io/conversations/123", status_code=404, json={"type": "error.list"})
            http_mocker.get("https://api.intercom.io/conversations/124", json=_conversation("124", _LIVE_TAGS))

            output = read()

        assert output.errors == []
        assert [record.record.data for record in output.records] == [{"id": "124", "updated_at": _UPDATED_AT, "tags": _LIVE_TAGS}]

    def test_emits_no_tags_and_no_other_field_when_tags_are_missing(self) -> None:
        conversation_without_tags = _conversation("123", _EMPTY_TAGS)
        del conversation_without_tags["tags"]

        with requests_mock.Mocker() as http_mocker:
            http_mocker.post("https://api.intercom.io/conversations/search", json=_search_response(["123"]))
            http_mocker.get("https://api.intercom.io/conversations/123", json=conversation_without_tags)

            output = read()

        assert [record.record.data for record in output.records] == [{"id": "123", "updated_at": _UPDATED_AT}]

    def test_emits_conversation_updated_before_the_stream_cursor(self) -> None:
        state = {
            "use_global_cursor": True,
            "state": {"updated_at": str(_UPDATED_AT + 300)},
            "lookback_window": 1,
            "parent_state": {"conversations": {"updated_at": str(_UPDATED_AT - 60)}},
        }

        with requests_mock.Mocker() as http_mocker:
            http_mocker.post("https://api.intercom.io/conversations/search", json=_search_response(["123"]))
            http_mocker.get("https://api.intercom.io/conversations/123", json=_conversation("123", _LIVE_TAGS))

            output = read(StateBuilder().with_stream_state(_STREAM_NAME, state))

        assert [record.record.data for record in output.records] == [{"id": "123", "updated_at": _UPDATED_AT, "tags": _LIVE_TAGS}]
