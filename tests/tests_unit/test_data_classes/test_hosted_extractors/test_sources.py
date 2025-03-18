import inspect
import textwrap

import pytest

from cognite.client.data_classes.hosted_extractors.sources import (
    _SOURCE_CLASS_BY_TYPE,
    _SOURCE_UPDATE_BY_TYPE,
    _SOURCE_WRITE_CLASS_BY_TYPE,
    BasicAuthentication,
    BasicAuthenticationWrite,
    RESTClientCredentialsAuthentication,
    RESTClientCredentialsAuthenticationWrite,
    RESTHeaderAuthentication,
    RESTHeaderAuthenticationWrite,
    RESTQueryAuthentication,
    RESTQueryAuthenticationWrite,
    RestSourceWrite,
    Source,
    SourceWrite,
)


class TestSource:
    def test_load_yaml_set_default_scheme(self) -> None:
        raw_yaml = textwrap.dedent(
            """\
            type: rest
            externalId: apim-sap_rest_source
            host: "<http://gateway.api.company.com|gateway.api.company.com>"
            port: 443
            authentication:
              type: "header"
              key: "Ocp-Apim-Subscription-Key"
              value: "super-secret-key"
            """
        )
        loaded = SourceWrite.load(raw_yaml)
        assert isinstance(loaded, RestSourceWrite)
        assert loaded.scheme == "https"


@pytest.fixture(
    params=[
        (
            {
                "source": "mqtt3",
                "externalId": "mqtt-source",
                "host": "mqtt-broker",
                "port": 1883,
                "authentication": {"type": "basic", "username": "user", "password": "pass"},
            },
            BasicAuthentication,
            BasicAuthenticationWrite,
        ),
        (
            {
                "source": "rest",
                "externalId": "rest-source",
                "host": "rest-host",
                "port": 443,
                "authentication": {"type": "basic", "username": "user", "password": "pass"},
            },
            BasicAuthentication,
            BasicAuthenticationWrite,
        ),
        (
            {
                "source": "rest",
                "externalId": "rest-source",
                "host": "rest-host",
                "port": 443,
                "authentication": {"type": "header", "key": "key", "value": "value"},
            },
            RESTHeaderAuthentication,
            RESTHeaderAuthenticationWrite,
        ),
        (
            {
                "source": "rest",
                "externalId": "rest-source",
                "host": "rest-host",
                "port": 443,
                "authentication": {"type": "query", "key": "key", "value": "value"},
            },
            RESTQueryAuthentication,
            RESTQueryAuthenticationWrite,
        ),
        (
            {
                "source": "rest",
                "externalId": "rest-source",
                "host": "rest-host",
                "port": 443,
                "authentication": {
                    "type": "clientCredentials",
                    "clientId": "client-id",
                    "clientSecret": "client-secret",
                    "tokenUrl": "https://token.url",
                    "scopes": ["scope1", "scope2"],
                },
            },
            RESTClientCredentialsAuthentication,
            RESTClientCredentialsAuthenticationWrite,
        ),
    ]
)
def sample_sources(request):
    return request.param


def test_auth_loaders_auth_cls(sample_sources):
    resource, expected_auth_cls, expected_auth_write_cls = sample_sources

    source_cls = _SOURCE_CLASS_BY_TYPE.get(resource["source"])
    resource["createdTime"] = "1970-01-01T00:00:00Z"
    resource["lastUpdatedTime"] = "1970-01-01T00:00:01Z"
    if resource.get("port", "") == 443:
        resource["scheme"] = "https"

    obj: Source = source_cls._load(resource=resource)
    assert isinstance(obj.authentication, expected_auth_cls)


def test_auth_loaders(sample_sources) -> None:
    resource, expected_auth_cls, expected_auth_write_cls = sample_sources

    source_write_cls = _SOURCE_WRITE_CLASS_BY_TYPE.get(resource["source"])
    obj: SourceWrite = source_write_cls._load(resource=resource)
    assert isinstance(obj.authentication, expected_auth_write_cls)


@pytest.mark.parametrize("source_type, source_write_class", _SOURCE_WRITE_CLASS_BY_TYPE.items())
def test_source_update_properties_match(source_type, source_write_class) -> None:
    source_write_cls = source_write_class
    source_update_cls = _SOURCE_UPDATE_BY_TYPE.get(source_type)

    write_properties = [
        param.name
        for param in inspect.signature(source_write_cls.__init__).parameters.values()
        if param.name not in ["self", "external_id"]
    ]
    update_properties = [property_spec.name for property_spec in source_update_cls._get_update_properties(None)]

    assert set(write_properties) == set(update_properties)
