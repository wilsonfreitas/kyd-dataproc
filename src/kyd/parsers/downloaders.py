
import os
import os.path
import logging
import tempfile
import zipfile
from datetime import datetime, timedelta
from abc import ABC, abstractmethod
import json

import pytz
import requests


def downloader_factory(**kwargs):
    if kwargs.get('type') is None:
        return RawURLDownloader(**kwargs)
    elif kwargs.get('type') == 'datetime':
        return FormatDateURLDownloader(**kwargs)
    elif kwargs.get('type') == 'prepared':
        return PreparedURLDownloader(**kwargs)


def download_by_config(config_data, save_func):
    logging.info('content size = %d', len(config_data))
    config = json.loads(config_data)
    logging.info('content = %s', config)
    downloader = downloader_factory(**config)
    fname, tfile, status_code = downloader.download()
    if status_code == 200:
        save_func(config, downloader.now, fname, tfile)


def get_fname(fname, attrs, refdate, alt_ext='html', date_format='%Y-%m-%d'):
    _date = refdate.strftime(date_format)
    prefix = attrs.get('prefix')
    ext = attrs.get('ext', alt_ext)
    if prefix:
        fname = '{}/{}'.format(prefix, fname) \
                if fname else '{}/{}.{}'.format(prefix, _date, ext)
    else:
        fname = fname if fname else '{}.{}'.format(_date, ext)
    return fname


def save_file_to_temp_folder(attrs, refdate, fname, tfile):
    fname = get_fname(fname, attrs, refdate)
    fname = '/tmp/{}'.format(fname)
    logging.info('saving file %s', fname)
    os.makedirs(os.path.dirname(fname), exist_ok=True)
    with open(fname, 'wb') as f:
        f.write(tfile.read())


class SingleDownloader(ABC):
    def __init__(self, **kwargs):
        self.attrs = kwargs
        self._url = None

    @property
    def now(self):
        tz = pytz.timezone('America/Sao_Paulo')
        return datetime.now(tz)

    @property
    def url(self):
        return self._url

    @abstractmethod
    def download(self):
        pass


class RawURLDownloader(SingleDownloader):
    def download(self):
        self._url = self.attrs['url']
        _, tfile, status_code = download_url(self._url)
        if status_code != 200:
            return None, None, status_code
        return None, tfile, status_code


class FormatDateURLDownloader(SingleDownloader):
    def download(self, refdate=None):
        refdate = refdate or self.now + \
                  timedelta(self.attrs.get('timedelta', 0))
        self._url = refdate.strftime(self.attrs['url'])
        _, tfile, status_code = download_url(self._url)
        if status_code != 200:
            return None, None, status_code
        _, ext = os.path.splitext(self._url)
        ext = ext if ext != '' else '.{}'.format(self.attrs['ext'])
        return '{}{}'.format(refdate.strftime('%Y-%m-%d'), ext), \
            tfile, \
            status_code


class PreparedURLDownloader(SingleDownloader):
    def download(self, refdate=None):
        url = self.attrs['url']
        refdate = refdate or self.now + \
            timedelta(self.attrs.get('timedelta', 0))
        params = {}
        for param in self.attrs['parameters']:
            param_value = self.attrs['parameters'][param]
            if isinstance(param_value, dict):
                if param_value['type'] == 'datetime':
                    params[param] = refdate.strftime(param_value['value'])
            else:
                params[param] = param_value

        self._url = url.format(**params)
        fname, temp_file, status_code = self._download_unzip_historical_data(
            self._url)
        if status_code != 200:
            return None, None, status_code
        return fname, temp_file, status_code

    def _download_unzip_historical_data(self, url):
        _, temp, status_code = download_url(url)
        if status_code != 200:
            return None, None, status_code
        zf = zipfile.ZipFile(temp)
        nl = zf.namelist()
        if len(nl) == 0:
            logging.error('zip file is empty url = {}'.format(url))
            return None, None, 404
        name = nl[0]
        content = zf.read(name)
        zf.close()
        temp.close()
        temp = tempfile.TemporaryFile()
        temp.write(content)
        temp.seek(0)
        return name, temp, status_code


def download_url(url):
    res = requests.get(url)
    msg = 'status_code = {} url = {}'.format(res.status_code, url)
    logg = logging.warn if res.status_code != 200 else logging.info
    logg(msg)
    if res.status_code != 200:
        return None, None, res.status_code
    temp = tempfile.TemporaryFile()
    temp.write(res.content)
    temp.seek(0)
    return None, temp, res.status_code
