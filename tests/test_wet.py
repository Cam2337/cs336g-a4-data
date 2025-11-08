import logging
from pathlib import Path

import pytest
import requests

from cs336_data import wet

logger = logging.getLogger(__name__)


def test_get_wet_file_urls():
    urls = wet.get_wet_file_urls()
    assert len(urls) == 5000
    # Expect always the same urls.
    assert (
        urls[0]
        == "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-43/segments/1759648667491.87/wet/CC-MAIN-20251018220658-20251019010658-00965.warc.wet.gz"
    )
    assert (
        urls[4999]
        == "https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-43/segments/1759648665892.58/wet/CC-MAIN-20251018062016-20251018092016-00140.warc.wet.gz"
    )


@pytest.mark.skip(reason="Not mocked - remove this line if you want to run this test.")
def test_download_wet_file(tmpdir):
    url = wet.get_wet_file_urls()[0]
    path = wet.download_wet_file(wet_url=url, dir=Path(tmpdir))
    assert path.exists()
    assert (
        path
        == Path(tmpdir)
        / "crawl-data/CC-MAIN-2025-43/segments/1759648667491.87/wet/CC-MAIN-20251018220658-20251019010658-00965.warc.wet.gz"
    )


def test_download_wet_file_raises():
    with pytest.raises(requests.ConnectionError):
        wet.download_wet_file(wet_url="https://404", dir=Path())
    with pytest.raises(requests.HTTPError):
        wet.download_wet_file(
            wet_url="https://data.commoncrawl.org/crawl-data/CC-MAIN-2025-43/segments/1759648667491.87/wet/CC-MAIN-THIS-FILE-DOES-NOT-EXIST.warc.wet.gz",
            dir=Path(),
        )


@pytest.mark.skip(reason="Not mocked - remove this line if you want to run this test.")
def test_open_wet_archive(tmpdir):
    url = wet.get_wet_file_urls()[0]
    path = wet.download_wet_file(wet_url=url, dir=Path(tmpdir))
    with wet.open_wet_archive(wet_file=path) as archive_iterator:
        for record in archive_iterator:
            contents = record.reader.read()
            assert isinstance(contents, bytes)
            assert len(contents)


@pytest.mark.skip(reason="Not mocked - remove this line if you want to run this test.")
def test_open_wet_archive_url():
    url = wet.get_wet_file_urls()[0]
    with wet.open_wet_archive_url(wet_url=url) as archive_iterator:
        for record in archive_iterator:
            contents = record.reader.read()
            assert isinstance(contents, bytes)
            assert len(contents)
