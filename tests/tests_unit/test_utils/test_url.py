from cognite.client.utils._url import interpolate_and_url_encode


def test_url_encode():
    assert "/bla/yes%2Fno/bla" == interpolate_and_url_encode("/bla/{}/bla", "yes/no")
    assert "/bla/123/bla/456" == interpolate_and_url_encode("/bla/{}/bla/{}", "123", "456")
