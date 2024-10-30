import textwrap

from cognite.client.data_classes.hosted_extractors import RestSourceWrite, SourceWrite


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
