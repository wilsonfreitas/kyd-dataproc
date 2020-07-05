
import re
import json
import logging

from kyd.etl import Dataset

from kyd.parsers.cvm import handle_informes_diarios


class INF_DIARIO_FI_RawDataset(Dataset):
    def get_data(self):
        all_data = {}
        for file in self.list_files():
            m = re.match(r'^.*inf_diario_fi_(\d{6})\.csv$', file)
            date = m.group(1)
            date = f'{date[:4]}-{date[4:6]}'
            logging.debug(file)
            with open(file) as fp:
                text = fp.read().strip()
                lines = text.split('\n')
                logging.debug(len(lines))
                data = {}
                for ix, line in enumerate(lines[1:]):
                    if line.strip():
                        key, row = handle_informes_diarios(line)
                        try:
                            data[key].append(row)
                        except KeyError:
                            data[key] = [row]
            all_data[date] = data
        return all_data


class INF_DIARIO_FI_MonthlyDataset(Dataset):
    def get_data(self):
        data = {}
        for file in self.list_files():
            m = re.match(r'^.*(\d{14}).(\d{4}-\d{2})\.json$', file)
            cnpj_fundo = m.group(1)
            with open(file) as fp:
                text = fp.read().strip()
                lines = text.split('\n')
                # logging.debug(len(lines))
                for ix, line in enumerate(lines[1:]):
                    if line.strip():
                        row = json.loads(line)
                        try:
                            data[cnpj_fundo].append(row)
                        except KeyError:
                            data[cnpj_fundo] = [row]
        return data
