
import re
import os
import json
import shutil
import logging
import zipfile

from datetime import datetime

import pytz
from google.cloud import storage

logging.basicConfig(level=logging.DEBUG)


class StagingArea:
    def __init__(self, path):
        self.path = path
        self.client = storage.Client()
        self.create()

    def create(self):
        if not os.path.exists(self.path):
            os.mkdir(self.path)

    def get(self, symbol, name):
        return None

    def drop(self):
        shutil.rmtree(self.path)
        logging.debug('staging area %s dropped', self.path)


def safe_mkdir(dir_name):
    if not os.path.exists(dir_name):
        os.mkdir(dir_name)


def dtfmt(dt, fmt='%Y-%m-%d %H:%M:%S.%f'):
    return datetime.strftime(dt, fmt)


def dtprs(dt, fmt='%Y-%m-%d %H:%M:%S.%f'):
    return datetime.strptime(dt, fmt).replace(tzinfo=pytz.UTC)


class DataPackage:
    def __init__(self, dataset_name, staging):
        self.dataset_name = dataset_name
        self.pack_name = f'{self.dataset_name}.zip'
        self.staging = staging
        self.destdir = staging.path
        self.local_pack_name = os.path.join(self.destdir, self.pack_name)
        self.destdir_pack = os.path.join(self.destdir, self.dataset_name)
        self.data_dir = os.path.join(self.destdir_pack, 'data')
        self.processed_fname = os.path.join(self.destdir_pack, '_PROCESSED')
        self.config_fname = os.path.join(self.destdir_pack, 'config.json')
        self.processed = []
        self.config = {}

    def download(self, blob_name):
        blob = storage.Blob.from_string(blob_name, client=self.staging.client)
        if blob.exists():
            with open(self.local_pack_name, 'wb+') as fp:
                blob.download_to_file(fp)
            if not self.local_pack_created:
                raise Exception('Pack exists but could not be downloaded')
            logging.debug(f'Pack {self.pack_name} downloaded to '
                          f'{self.local_pack_name}')
        else:
            logging.debug(f'Pack {self.pack_name} does not exist')

    @property
    def local_pack_created(self):
        return os.path.exists(self.local_pack_name)

    def unpack(self):
        if self.local_pack_created:
            shutil.rmtree(self.destdir_pack)
            with zipfile.ZipFile(self.local_pack_name) as zip_ref:
                nl = zip_ref.namelist()
                if len(nl) < 2:
                    raise Exception('Invalid pack: number of elements not '
                                    'greater than 2')
                logging.debug(f'Pack number of elements ckeck: OK')
                if '_PROCESSED' not in nl:
                    raise Exception(f'Invalid pack: register _PROCESSED '
                                    f'not found')
                if 'config.json' not in nl:
                    raise Exception(f'Invalid pack: config.json '
                                    f'not found')
                logging.debug(f'Pack register ckeck: OK')
                zip_ref.extractall(self.destdir_pack)
                logging.debug(f'Pack extracted to {self.destdir_pack}')
            if not os.path.exists(self.data_dir):
                os.mkdir(self.data_dir)
            if not self.check():
                raise Exception('Invalid pack: inconsistent number of '
                                'processed files')
        else:
            raise FileNotFoundError(f'{self.local_pack_name} not found')

    def load(self):
        with open(self.processed_fname) as fp:
            p = json.loads(fp.read())
            self.processed = [(d[0], dtprs(d[1])) for d in p]
            logging.debug(f'Pack {self.local_pack_name} has '
                          f'{len(self.processed)} processed files')
        with open(self.config_fname) as fp:
            self.config = json.loads(fp.read())
            logging.debug(f'Pack {self.local_pack_name} loaded config')
        if not self.check():
            raise Exception('Invalid pack: inconsistent number of '
                            'processed files')

    def update(self):
        source_bucket = storage.Bucket(self.staging.client,
                                       self.config['source_bucket'])
        blobs = source_bucket.list_blobs(prefix=self.config['source_prefix'])
        blobs = {b.name: b for b in blobs}
        # check against blob names
        input_bnames = set(blobs)
        logging.debug('input blobs = %s', len(input_bnames))
        old_processed = set(p[0] for p in self.processed)
        logging.debug('blobs already processed = %s', len(old_processed))
        bnames_to_process = list(input_bnames - old_processed)
        logging.debug('non processed blobs = %s', len(bnames_to_process))
        # check against timestamps
        bnames_to_process2 = [b for b, t in self.processed
                              if blobs[b].time_created > t]
        logging.debug('outdated blobs to process = %s',
                      len(bnames_to_process2))
        # select blobs to process
        bnames_to_process = set(bnames_to_process + bnames_to_process2)
        logging.debug('blobs to process = %s', len(bnames_to_process))
        procs = []
        for bname in bnames_to_process:
            blob = blobs[bname]
            fname = os.path.split(bname)[1]
            fname = os.path.join(self.data_dir, fname)
            with open(fname, 'wb+') as fp:
                blob.download_to_file(fp)
            procs.append((blob.name, dtfmt(blob.time_created)))
        if procs:
            logging.debug('updating processed %s', self.processed_fname)
            self.processed = self.processed + procs
            with open(self.processed_fname, 'w+') as fp:
                fp.write(json.dumps(self.processed))

    def check(self):
        chk = True
        logging.debug('Pack consistency check: %s', 'OK' if chk else 'FAIL')
        return chk

    def pack(self):
        if not self.check():
            raise Exception('Invalid pack: inconsistent number of processed '
                            'files')
        cd = os.getcwd()
        os.chdir(self.destdir_pack)
        with zipfile.ZipFile(self.local_pack_name, 'w') as zip_ref:
            for root, dirs, files in os.walk(r'.'):
                for file in files + dirs:
                    fname = os.path.join(root, file)
                    zip_ref.write(fname)
        os.chdir(cd)

    def upload(self, blob_name):
        blob = storage.Blob.from_string(blob_name, client=self.staging.client)
        blob.upload_from_filename(self.local_pack_name)

    def create(self, config={}):
        self.config = config
        self.processed = []
        safe_mkdir(self.destdir_pack)
        safe_mkdir(self.data_dir)
        with open(self.processed_fname, 'w+') as fp:
            fp.write(json.dumps(self.processed))
        with open(self.config_fname, 'w+') as fp:
            fp.write(json.dumps(self.config))

    def save_data(self, data, filename, processed):
        fname = os.path.join(self.data_dir, filename)
        dir = os.path.split(fname)[0]
        os.makedirs(dir, exist_ok=True)
        content = [json.dumps(c)+'\n' for c in data]
        with open(fname, 'w+') as fp:
            fp.writelines(content)
        # logging.debug('updating processed %s', self.processed_fname)
        self.processed = processed
        with open(self.processed_fname, 'w+') as fp:
            fp.write(json.dumps(self.processed))

    def deploy_data(self, staging, dataset_name=None):
        dataset_name = dataset_name if dataset_name else self.dataset_name
        dst = os.path.join(staging.path, dataset_name)
        for root, dirs, files in os.walk(self.data_dir):
            dst_dir = root.replace(self.data_dir, dst, 1)
            safe_mkdir(dst_dir)
            for file in files:
                src_fname = os.path.join(root, file)
                dst_fname = os.path.join(dst_dir, file)
                logging.debug(src_fname)
                logging.debug(dst_fname)
                shutil.copy(src_fname, dst_fname)


class Dataset:
    def __init__(self, name, staging):
        self.package = DataPackage(name, staging)
        self.dataset_name = name
        self.staging = staging
        self.destdir = staging.path
        self.dataset_path = os.path.join(self.destdir, self.dataset_name)
        self.data_dir = os.path.join(self.dataset_path, 'data')

    def list_files(self):
        fnames = []
        for root, dirs, files in os.walk(self.data_dir):
            for file in files:
                fnames.append(os.path.join(root, file))
        return fnames

    def get_data_files(self):
        def adj_fname(fname):
            fname = fname.replace(self.destdir, '', 1)
            return fname[1:] if fname.startswith(os.path.sep) else fname

        def mtime(fname):
            ts = os.path.getmtime(fname)
            return dtfmt(datetime.utcfromtimestamp(ts))

        return [(adj_fname(fname), mtime(fname))
                for fname in self.list_files()]

    def save_data(self, **kwargs):
        self.package.save_data(kwargs['data'], kwargs['filename'],
                               kwargs['processed'])

    def get_data(self):
        raise NotImplementedError('Dataset.get_data not implemented')


class SeriesDataset(Dataset):
    def get_data(self):
        with open(os.path.join(self.data_dir, 'series.json')) as fp:
            series = [json.loads(line.strip()) for line in fp.readlines()]
        return series

    def save_data(self, **kwargs):
        self.package.save_data(kwargs['data'], 'series.json',
                               kwargs['processed'])
