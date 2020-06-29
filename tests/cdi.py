
import re
import os
import sys
import json
import logging
import datetime

from google.cloud import storage

from kyd.data.b3 import parse_taxa_cdi_idi


def _save_text_to_bucket(output_bucket_name, fname, text):
    logging.info('saving file %s/%s', output_bucket_name, fname)
    storage_client = storage.Client()
    output_bucket = storage_client.get_bucket(output_bucket_name)
    output_blob = output_bucket.blob(fname)
    output_blob.upload_from_string(text)


def gen_blobs():
    storage_client = storage.Client()
    config_bucket = storage_client.get_bucket('ks-rawdata-b3')
    blobs = list(config_bucket.list_blobs(prefix='TaxaCDI'))
    for blob in blobs:
        text = blob.download_as_string()
        cdi_data, _ = parse_taxa_cdi_idi(text)
        yield cdi_data


def gen_blobs2():
    # {"trade_date": "2020-04-20", "last_price": 3.65, "ticker_symbol": "CDI"}
    cetip_dir = '/Volumes/Data/cetip/MediaCDI'
    fname_re = re.compile(r'(\d{8})\.txt')
    files = [d for d in os.listdir(cetip_dir) if d.endswith('.txt')]
    for fname in files:
        logging.info('processing file %s', fname)
        m_fname = fname_re.match(fname)
        if m_fname is None:
            logging.warn('invalid filename %s', fname)
            continue
        with open(os.path.join(cetip_dir, fname)) as fp:
            text = fp.readline()
        text = text.strip()
        rate = int(text) / 100
        # date = datetime.datetime.strptime('%Y%m%d').strftime('%Y-%m-%d')
        date = '{}-{}-{}'.format(fname[:4], fname[4:6], fname[6:8])
        yield {
            'trade_date': date,
            'last_price': rate,
            'ticker_symbol': 'CDI'
        }


def gen_blobs3():
    storage_client = storage.Client()
    config_bucket = storage_client.get_bucket('ks-json-bvmf')
    blobs = list(config_bucket.list_blobs(prefix='prices/CDI'))
    for blob in blobs:
        logging.info('processing %s', blob.name)
        t1 = datetime.datetime.now()
        text = blob.download_as_string()
        eps = datetime.datetime.now() - t1
        print(eps.total_seconds())
        cdi_data = json.loads(text)
        yield cdi_data


logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    # for data in gen_blobs():
    #     fname = 'prices/CDI/{}.json'.format(data['trade_date'])
    #     _save_text_to_bucket('ks-json-bvmf', fname, json.dumps(data))
    # epss = []
    # for data in gen_blobs2():
    #     fname = 'prices/CDI/{}.json'.format(data['trade_date'])
    #     # print(fname)
    #     logging.info(data)
    #     t1 = datetime.datetime.now()
    #     _save_text_to_bucket('ks-json-bvmf', fname, json.dumps(data))
    #     eps = datetime.datetime.now() - t1
    #     epss.append(eps)
    # for eps in epss:
    #     print(eps.total_seconds())
    fulldata = [_ for _ in gen_blobs3()]
    data = [[x['trade_date'], x['last_price']] for x in fulldata]
    data.sort(key=lambda x: x[0])
    series = {
        'symbol': fulldata[0]['ticker_symbol'],
        'header': {
            'start_date': data[0][0],
            'end_date': data[-1][0],
            'columns': ['refdate', 'value']
        },
        'data': data
    }
    fname = 'prices/CDI/series.json'
    _save_text_to_bucket('ks-json-bvmf', fname, json.dumps(series))
    logging.info('saved %s to %s', fname, 'ks-json-bvmf')
