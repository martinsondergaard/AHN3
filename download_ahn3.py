import io
import json
import multiprocessing as mp
from pathlib import Path
from zipfile import ZipFile

import requests
from tqdm import tqdm

OUTDIR = 'AHN3'
WORKERS = 20


def get_download_urls():
    urls = []
    r = requests.get(
        'https://service.pdok.nl/rws/ahn3/wfs/v1_0?version=2.0.0&request=GetFeature&service=WFS&typename=ahn3:ahn3_bladindex&outputformat=json'
    )
    data = json.loads(r.content)
    for feature in data['features']:
        properties = feature['properties']
        bladnr = properties['bladnr'].upper()
        dtype = '5m_dsm'
        prefix = 'R5'
        if properties[f'has_data_{dtype}']:
            urls.append(
                f'https://download.pdok.nl/rws/ahn3/v1_0/{dtype}/{prefix}_{bladnr}.ZIP'
            )
    return urls


def download_and_extract(url):
    response = requests.get(url)
    with io.BytesIO() as tempfile:
        tempfile.write(response.content)
        with ZipFile(tempfile) as zipfile:
            zipfile.extractall(OUTDIR)


def main() -> None:
    out = Path(OUTDIR)
    out.mkdir(exist_ok=True)
    urls = get_download_urls()

    # WITHOUT PROGRESS BAR
    # for url in urls:
    #     download_and_extract(url)

    # WITH PROGRESS BAR AND MULTIPROCESSING
    with mp.Pool(processes=WORKERS) as pool:
        generator = tqdm(
            pool.imap_unordered(download_and_extract, urls, chunksize=1),
            total=len(urls),
            )
        for _ in generator:
            pass


if __name__ == '__main__':
    main()
