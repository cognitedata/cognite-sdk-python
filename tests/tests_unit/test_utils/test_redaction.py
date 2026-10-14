from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest
from httpx2 import Headers

from cognite.client.data_classes.hosted_extractors.sources import (
    AuthCertificateWrite,
    BasicAuthenticationWrite,
    EventHubSourceWrite,
    RESTHeaderAuthenticationWrite,
    RestSourceWrite,
)
from cognite.client.data_classes.iam import ClientCredentials
from cognite.client.utils._redaction import redact, redact_headers, redact_response_body, sensitive_fields

SECRET = "PLANTED-SECRET-VALUE"


class TestRedact:
    @pytest.mark.parametrize(
        "payload, expected",
        [
            pytest.param(
                {"items": [ClientCredentials("my-client-id", SECRET).dump()]},
                {"items": [{"clientId": "my-client-id", "clientSecret": "***"}]},
                id="listed field, found by name",
            ),
            pytest.param(
                # Verbatim from a real debug log, where these leaked in plaintext. Only the values
                # are secret: the names are whatever the user called them, and the API hands them
                # back the same way.
                {"items": [{"functionPath": "handler.py", "secrets": {"first": "yoyo", "second": "nono"}}]},
                {"items": [{"functionPath": "handler.py", "secrets": {"first": "***", "second": "***"}}]},
                id="credential mapping keeps its names",
            ),
            pytest.param(
                {
                    "items": [
                        RestSourceWrite(
                            external_id="my-source",
                            host="example.com",
                            authentication=BasicAuthenticationWrite(username="usr", password=SECRET),
                        ).dump()
                    ]
                },
                {
                    "items": [
                        {
                            "externalId": "my-source",
                            "host": "example.com",
                            "scheme": "https",
                            "authentication": {"username": "usr", "password": "***", "type": "basic"},
                            "type": "rest",
                        }
                    ]
                },
                id="listed field on a nested object",
            ),
            pytest.param(
                RESTHeaderAuthenticationWrite(key="X-Api-Key", value=SECRET).dump(),
                # The header *name* is not a secret, only its value
                {"key": "X-Api-Key", "value": "***", "type": "header"},
                id="generic name, inside the type that declared it",
            ),
            pytest.param(
                AuthCertificateWrite(type="pem", certificate="public-cert", key=SECRET, key_password=SECRET).dump(),
                {"type": "pem", "certificate": "public-cert", "key": "***", "keyPassword": "***"},
                id="generic name, inside a certificate",
            ),
            pytest.param(
                {"items": [{"externalId": "ts", "datapoints": [{"timestamp": 1, "value": 4.2}]}]},
                {"items": [{"externalId": "ts", "datapoints": [{"timestamp": 1, "value": 4.2}]}]},
                id="same generic name, but no type to go with it",
            ),
            pytest.param(
                EventHubSourceWrite(
                    external_id="my-source", host="h", event_hub_name="hub", key_name="my-key-name", key_value=SECRET
                ).dump(),
                # 'keyName' looks similar but holds the name of the key, not the key itself
                {
                    "externalId": "my-source",
                    "host": "h",
                    "eventHubName": "hub",
                    "keyName": "my-key-name",
                    "keyValue": "***",
                    "type": "eventhub",
                },
                id="listed field next to a similar-looking one",
            ),
            pytest.param(
                # Regression guard: plenty of field names contain "token" without being sensitive
                {
                    "items": [{"tokenExchange": True}],
                    "authentication": {"type": "clientCredentials", "tokenUrl": "https://login.example.com/token"},
                },
                {
                    "items": [{"tokenExchange": True}],
                    "authentication": {"type": "clientCredentials", "tokenUrl": "https://login.example.com/token"},
                },
                id="'token' in a name does not make it a secret",
            ),
            pytest.param(
                # A dict built by hand and passed to client.post() has no data class behind it, so
                # the field name is all we have to go by.
                {"authentication": {"nonce": SECRET}, "myClientSecret": SECRET},
                {"authentication": {"nonce": "***"}, "myClientSecret": "***"},
                id="payload we did not produce ourselves",
            ),
            pytest.param(
                {"items": ({"clientSecret": SECRET},)},
                {"items": [{"clientSecret": "***"}]},
                id="sequence that is not a list",
            ),
        ],
    )
    def test_redacts(self, payload: dict[str, Any], expected: dict[str, Any]) -> None:
        assert redact(payload) == expected

    @pytest.mark.parametrize(
        "payload",
        [
            {"secret_maybe": SECRET},  # root level
            {"items": [{"clientSecret": SECRET}]},  # nested
            {"authentication": {"nonce": SECRET, "myClientSecret": SECRET}},  # multiple keys
        ],
    )
    def test_does_not_touch_the_payload_it_was_given(self, payload: dict[str, Any]) -> None:
        # Ensure we never mutate the original payload, as that would be pretty catastrophic...
        still_payload = deepcopy(payload)
        assert SECRET in str(payload)

        redacted = redact(payload)
        assert SECRET not in str(redacted)  # also ensure we actually did something to it

        assert payload == still_payload


class TestRedactHeaders:
    @pytest.mark.parametrize("header_type", [Headers, dict])
    def test_only_credential_headers_are_redacted(self, header_type: Any) -> None:
        before = header_type({"Authorization": "bla", "key": "bla"})
        after = header_type({"Authorization": "***", "key": "bla"})

        assert before != after
        assert after == redact_headers(before)


class TestStrAndRepr:
    @pytest.mark.parametrize(
        "obj",
        [
            pytest.param(ClientCredentials("my-client-id", SECRET), id="resource"),
            pytest.param(
                EventHubSourceWrite(external_id="x", host="h", event_hub_name="hub", key_name="kn", key_value=SECRET),
                id="resource with a generic-sounding field",
            ),
            pytest.param(BasicAuthenticationWrite(username="usr", password=SECRET), id="dataclass"),
            pytest.param(
                AuthCertificateWrite(type="pem", certificate="c", key=SECRET, key_password=SECRET),
                id="dataclass, two listed fields",
            ),
        ],
    )
    def test_secret_shows_up_in_neither(self, obj: Any) -> None:
        # A notebook echo, a log line or a failing assert is enough to leak these, no debug
        # logging needed, so both str() and repr() have to be safe.
        assert SECRET not in str(obj)
        assert SECRET not in repr(obj)

    def test_everything_else_stays_visible(self) -> None:
        assert "my-client-id" in str(ClientCredentials("my-client-id", SECRET))


class TestRedactResponseBody:
    @pytest.mark.parametrize(
        "body, expected",
        [
            pytest.param(
                f'{{"items":[{{"id":1,"status":"READY","nonce":"{SECRET}"}}]}}',
                '{"items":[{"id":1,"status":"READY","nonce":"***"}]}',
                id="nonce returned by iam.sessions.create()",
            ),
            pytest.param(
                f'{{"items":[{{"host":"h","username":"u","password":"{SECRET}"}}]}}',
                '{"items":[{"host":"h","username":"u","password":"***"}]}',
                id="password returned by postgres_gateway.users.create()",
            ),
            pytest.param(
                f'{{"items":[{{"secrets":{{"first":"{SECRET}"}}}}]}}',
                '{"items":[{"secrets":{"first":"***"}}]}',
                id="secrets, were the API ever to return them",
            ),
            pytest.param(
                # The API masks the values itself and returns the names, so there is nothing left
                # for us to do - and we must not collapse it to "***" and lose the names.
                '{"items":[{"secrets":{"first":"***"},"status":"Queued"}]}',
                '{"items":[{"secrets":{"first":"***"},"status":"Queued"}]}',
                id="secrets already masked by the API",
            ),
            pytest.param(
                # A proxy's error page, protobuf, plain text... Left alone on purpose: everything
                # that returns a credential answers in JSON, so there is nothing here to protect.
                "<html><body>502 Bad Gateway</body></html>",
                "<html><body>502 Bad Gateway</body></html>",
                id="body we cannot parse",
            ),
        ],
    )
    def test_redacts(self, body: str, expected: str) -> None:
        assert redact_response_body(body) == expected


class TestRegistry:
    def test_declarations_are_collected(self) -> None:
        fields = sensitive_fields()

        # Both spellings, since dump() is called both ways
        assert {"clientSecret", "client_secret", "keyValue", "key_value", "secrets", "nonce"} <= fields.anywhere

        # Generic names are only known together with the type that declared them:
        assert "value" not in fields.anywhere
        assert "value" in fields.by_discriminator["header"]
        assert "key" in fields.by_discriminator["pem"]
