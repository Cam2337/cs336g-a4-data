import contextlib
import gzip
import random
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path
from urllib import parse

import requests
from fastwarc.warc import ArchiveIterator

_PATHS_GZ = Path("./data/common_crawl_september_2025_wet.paths.gz")
_URL_PREFIX = "https://data.commoncrawl.org"
_N_URLS = 5000
_SEED = 336


def get_wet_file_urls() -> list[str]:
    """Returns urls for all 5000 .wet files that can be used in the assignment."""
    if not _PATHS_GZ.exists():
        raise RuntimeError("Can't load .wet file urls. Paths file does not exist!")
    with gzip.open(_PATHS_GZ) as raw_urls:
        urls = [url.strip().decode() for url in raw_urls]
    random.seed(_SEED)
    random.shuffle(urls)
    return [f"{_URL_PREFIX}/{url}" for url in urls[:_N_URLS]]


def download_wet_file(wet_url: str, dir: Path) -> Path:
    """Downloads a .wet file to a directory.

    For a given directory /data/ this will download the file to:
    /data/crawl-data/CC-MAIN-2025-43/segments/{SEGMENT}/wet/CC-MAIN-{ID}-{TIMESTAMP}-{INDEX}.warc.wet.gz

    Args:
        wet_url:
            Url pointing to the .wet file to download.
        dir:
            Directory to download the file to.

    Returns:
        Path to the downloaded file.

    Raises:
        ConnectionError or HTTPError if downloading the file fails.
    """
    parsed_url = parse.urlparse(wet_url)
    url_path = Path(parsed_url.path)
    output_path = dir / url_path.relative_to(url_path.anchor)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(wet_url, stream=True) as response:
        response.raise_for_status()
        output_path.write_bytes(response.content)
    return output_path


@contextlib.contextmanager
def open_wet_archive(wet_file: Path) -> Iterator[ArchiveIterator]:
    """Context manager for an iterator over a .wet file on disk.

    Usage:
    ```python
    wet_file = download_wet_file(wet_url=wet_url, dir=dir)
    with open_wet_archive(wet_file=wet_file) as archive_iterator:
        for record in archive_iterator:
            contents = record.reader.read()
    ```
    """
    stream = wet_file.open("rb")
    archive_iterator = ArchiveIterator(stream=stream)
    try:
        yield archive_iterator
    finally:
        stream.close()


@contextlib.contextmanager
def open_wet_archive_url(wet_url: str) -> Iterator[ArchiveIterator]:
    """Context manager for an iterator over a downloadable .wet file.

    This downloads the .wet file to a temporary directory. Upon exiting
    the context, the file and directory are deleted again.

    Usage:
    ```python
    with open_wet_archive_url(wet_url=wet_url) as archive_iterator:
        for record in archive_iterator:
            contents = record.reader.read()
    ```
    """
    temp_dir = Path(tempfile.mkdtemp())
    temp_file = download_wet_file(wet_url=wet_url, dir=temp_dir)
    try:
        with open_wet_archive(wet_file=temp_file) as archive_iterator:
            yield archive_iterator
    finally:
        temp_file.unlink()
        shutil.rmtree(temp_dir)
