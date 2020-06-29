
import os
import json

from kyd.etl import Dataset

from kyd.parsers.b3 import parse_taxa_cdi_idi_cetip


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
