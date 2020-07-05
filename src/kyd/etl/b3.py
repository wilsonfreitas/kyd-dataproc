
import os
import json

from kyd.etl import Dataset

from kyd.parsers.b3 import parse_taxa_cdi_idi_cetip, parse_taxa_cdi_idi_b3


class CDI_CETIP_RawDataset(Dataset):
    def get_data(self):
        dataset = []
        for file in self.list_files():
            with open(file) as fp:
                text = fp.read().strip()
            name = os.path.split(file)[1]
            rate, date = parse_taxa_cdi_idi_cetip(name, text)
            data = {
                'refdate': date,
                'value': rate,
                'symbol': 'CDI'
            }
            dataset.append(data)
        return dataset


class CDI_B3_RawDataset(Dataset):
    # {"taxa":"4,90","dataTaxa":"29/11/2019","indice":"31.296,02","dataIndice":"02/12/2019"}
    def get_data(self):
        dataset = []
        for file in self.list_files():
            with open(file) as fp:
                text = fp.read().strip()
            cdi_data, _ = parse_taxa_cdi_idi_b3(text)
            dataset.append(cdi_data)
        return dataset
