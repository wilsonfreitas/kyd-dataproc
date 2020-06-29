

import os
import shutil
import re
import json
import zipfile
import tempfile
import logging

from datetime import datetime

import pytz
from google.cloud import storage

logging.basicConfig(level=logging.DEBUG)


def dtfmt(dt, fmt='%Y-%m-%d %H:%M:%S.%f'):
    return datetime.strftime(dt, fmt)


def dtprs(dt, fmt='%Y-%m-%d %H:%M:%S.%f'):
    return datetime.strptime(dt, fmt).replace(tzinfo=pytz.UTC)


class ProcessedRegister:
    key = '_PROCESSED'

    def __init__(self, bucket, path, name):
        self.bucket = bucket
        self.pack_name = f'{path}/{self.key}-{name}.zip'
        # ----
        self.destdir = tempfile.mkdtemp()
        self.destdir_pack = os.path.join(self.destdir, '_pack')
        if not os.path.exists(self.destdir_pack):
            os.mkdir(self.destdir_pack)
        self.destdir_subpacks = os.path.join(self.destdir, '_subpacks')
        if not os.path.exists(self.destdir_subpacks):
            os.mkdir(self.destdir_subpacks)
        self.local_pack_name = os.path.join(self.destdir, f'{self.key}.zip')
        self.processed_fname = os.path.join(self.destdir_pack, self.key)
        # ----
        self.local_pack_created = False
        self.processed = []
        # actions ----
        self.download()
        if self.local_pack_created:
            with zipfile.ZipFile(self.local_pack_name) as zip_ref:
                nl = zip_ref.namelist()
                if len(nl) < 2:
                    raise Exception('Invalid pack: number of elements not '
                                    'greater than 2')
                logging.debug(f'Pack number of elements ckeck: OK')
                if self.key not in nl:
                    raise Exception(f'Invalid pack: register {self.key} '
                                    f'not found')
                logging.debug(f'Pack register ckeck: OK')
                zip_ref.extractall(self.destdir_pack)
                logging.debug(f'Pack extracted to {self.destdir_pack}')
            with open(self.processed_fname) as fp:
                p = json.loads(fp.read())
                self.processed = [(d[0], dtprs(d[1])) for d in p]
                logging.debug(f'Pack {self.local_pack_name} has '
                              f'{len(self.processed)} processed files')
            if not self.check():
                raise Exception('Invalid pack: inconsistent number of '
                                'processed files')

    def check(self):
        chk = len(self.processed) == len(os.listdir(self.destdir_pack))-1
        logging.debug('Pack consistency check: %s', 'OK' if chk else 'FAIL')
        return chk

    def download(self):
        processed_blob = storage.Blob(self.pack_name, self.bucket)
        if processed_blob.exists(client=self.bucket.client):
            with open(self.local_pack_name, 'wb+') as fp:
                processed_blob.download_to_file(fp)
            self.local_pack_created = os.path.exists(self.local_pack_name)
            if not self.local_pack_created:
                raise Exception('Pack exists but could not be downloaded')
            logging.debug(f'Pack {self.pack_name} downloaded to '
                          f'{self.local_pack_name}')
        else:
            logging.debug(f'Pack {self.pack_name} does not exist')

    def upload(self):
        processed_blob = storage.Blob(self.pack_name, self.bucket)
        processed_blob.upload_from_filename(self.local_pack_name,
                                            client=self.bucket.client)

    def update(self, processed):
        logging.debug('updating processed %s', self.processed_fname)
        self.processed = [(p[0], dtfmt(p[1])) for p in processed]
        with open(self.processed_fname, 'w+') as fp:
            fp.write(json.dumps(self.processed))

    def pack(self):
        if not self.check():
            raise Exception('Invalid pack: inconsistent number of processed '
                            'files')
        cd = os.getcwd()
        os.chdir(self.destdir_pack)
        with zipfile.ZipFile(self.local_pack_name, 'w') as zip_ref:
            for fname in os.listdir('.'):
                zip_ref.write(fname)
        os.chdir(cd)

    def save(self, data, date, ext='.json', subdir=''):
        if isinstance(data, str):
            self._save_single(data, date, ext, subdir)
        elif isinstance(data, dict):
            self._save_many(data, date, ext, subdir)

    def _save_single(self, data, date, ext='.json', subdir=''):
        dst = self.destdir_pack
        if subdir and not os.path.exists(os.path.join(dst, subdir)):
            os.mkdir(os.path.join(dst, subdir))
        fname = os.path.join(dst, subdir, f'{date}{ext}')
        logging.debug('saving %s', fname)
        with open(fname, 'w+') as fp:
            fp.write(data+'\n')

    def _save_many(self, data, date, ext='.json', subdir=''):
        dst = self.destdir_subpacks
        for ks in data:
            if not os.path.exists(os.path.join(dst, ks)):
                os.mkdir(os.path.join(dst, ks))
            fname = os.path.join(dst, ks, f'{date}{ext}')
            logging.debug('saving %s', fname)
            with open(fname, 'w+') as fp:
                fp.writelines([ds+'\n' for ds in data[ks]])

    def files(self, ext='.json'):
        for fname in os.listdir(self.destdir_pack):
            if not fname.endswith(ext):
                continue
            fname = os.path.join(self.destdir_pack, fname)
            yield fname

    def cleanup(self):
        logging.debug('removing %s', self.destdir)
        # shutil.rmtree(self.destdir)


class _Handler:
    def __init__(self, config):
        self.config = config
        self.client = storage.Client()
        self.input_bucket = storage.Bucket(self.client,
                                           self.config['input_bucket'])
        self.output_bucket = storage.Bucket(self.client,
                                            self.config['output_bucket'])
        self.reg = ProcessedRegister(self.output_bucket,
                                     self.config['symbol'],
                                     self.config['name'])
        self.old_processed_bnames = [p[0] for p in self.reg.processed]
        self.new_processed_bnames = []
        self.input_blobs = []

    def execute(self):
        self.list_input()
        self.process_blobs()
        if len(self.new_processed_bnames) > 0:
            # update _PROCESSED
            processed = [(bn, self.input_blobs[bn].time_created)
                         for bn in self.input_blobs]
            self.reg.update(processed)
            # create _PROCESSED.zip
            self.reg.pack()
            self.reg.upload()
        self.reg.cleanup()

    def list_input(self):
        prefix = self.config['input_prefix']
        blobs = self.input_bucket.list_blobs(prefix=prefix)
        blobs = {b.name: b for b in blobs}
        if self.config.get('input_regex'):
            rxd = re.compile(self.config['input_regex'])
            ms = [x is not None for x in [rxd.match(bn) for bn in blobs]]
            self.input_blobs = {bn: blobs[bn] for m, bn in zip(ms, blobs) if m}
        else:
            self.input_blobs = blobs

    def process_blobs(self):
        # check against blob names
        input_bnames = set(self.input_blobs)
        logging.debug('input blobs = %s', len(input_bnames))
        old_processed = set(self.old_processed_bnames)
        logging.debug('blobs already processed = %s', len(old_processed))
        bnames_to_process = list(input_bnames - old_processed)
        logging.debug('non processed blobs = %s', len(bnames_to_process))
        # check against timestamps
        bnames_to_process2 = [b for b, t in self.reg.processed
                              if self.input_blobs[b].time_created > t]
        logging.debug('outdated blobs to process = %s',
                      len(bnames_to_process2))
        # select blobs to process
        bnames_to_process = set(bnames_to_process + bnames_to_process2)
        logging.debug('blobs to process = %s', len(bnames_to_process))

        for bname in bnames_to_process:
            blob = self.input_blobs[bname]
            text = blob.download_as_string()
            self.handle_processed_data(blob.name, text)
            self.new_processed_bnames.append(blob.name)

    def get_data(self, bname, text):
        raise NotImplementedError()

    def handle_processed_data(self, name, text):
        raise NotImplementedError()


class OneToOneHandler(_Handler):
    def handle_processed_data(self, name, text):
        data, date = self.get_data(name, text)
        self.reg.save(data, date)


class OneToManyHandler(_Handler):
    def handle_processed_data(self, name, text):
        data, date = self.get_data(name, text)
        self.reg.save(data, date)


class SeriesComposer:
    def __init__(self, config):
        self.config = config
        self.client = storage.Client()
        self.input_bucket = storage.Bucket(self.client,
                                           self.config['input_bucket'])
        self.output_name = self.config.get('output_name') \
            if self.config.get('output_name') \
            else 'series.json'
        self.reg = ProcessedRegister(self.input_bucket,
                                     self.config['symbol'],
                                     self.config['source_name'])

    def get_output_blob_name(self):
        return f'{self.config["symbol"]}/{self.output_name}'

    def execute(self):
        series = []
        for fname in self.reg.files():
            with open(fname) as fp:
                data = json.loads(fp.read())
            series.append(data)
        series.sort(key=lambda x: x['refdate'])

        logging.debug('generating series file with %s rows', len(series))

        def dump_n_convert(data):
            s = json.dumps(data)+'\n'
            return s.encode('utf_8')

        temp = tempfile.TemporaryFile('wb+')
        lines = [dump_n_convert(data) for data in series]
        temp.writelines(lines)
        temp.seek(0)

        logging.debug('uploading series file to %s',
                      self.get_output_blob_name())
        series_blob = storage.Blob(self.get_output_blob_name(),
                                   self.input_bucket)
        series_blob.upload_from_file(temp, client=self.client)
        self.reg.cleanup()
