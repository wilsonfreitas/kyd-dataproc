
import os
import context

from kyd.etl import StagingArea, DataPackage
from kyd.etl.b3 import CDI_CETIP_RawDataset
from kyd.etl.cvm import INF_DIARIO_FI_RawDataset, INF_DIARIO_FI_MonthlyDataset

final_staging = StagingArea(r'C:\Users\wilso\Dev\google-cloud\kyd-dataproc'
                            r'\tests\final-staging')
final_staging.drop()
final_staging.create()

staging_area = StagingArea(r'C:\Users\wilso\Dev\google-cloud\kyd-dataproc'
                           r'\tests\staging')
staging_area.drop()
staging_area.create()

dp = DataPackage('FUNDOS_CVM-inf_diario_fi_rawdata', staging_area)
dp.create({
    'source_bucket': 'ks-tmp',
    'source_prefix': 'INF-DIARIO-FI'
})
dp.load()
dp.update()
# dp.pack()
# dp.upload('gs://ks-tmp/FUNDOS_CVM-inf_diario_fi_rawdata.zip')

ds = INF_DIARIO_FI_RawDataset('FUNDOS_CVM-inf_diario_fi_rawdata', staging_area)

dps = {}
for ix in range(0, 10):
    key = str(ix)
    dps[key] = DataPackage('FUNDOS_CVM-inf_diario_fi_arquivos_mes' + key,
                           staging_area)
    dps[key].create()

funds = ds.get_data()
for dx in funds:
    for fx in funds[dx]:
        key = fx[-1]
        dps[key].save_data(funds[dx][fx], os.path.join(fx, f'{dx}.json'),
                           ds.get_data_files())

dps1 = {}
for ix in range(0, 10):
    key = str(ix)
    dps1[key] = DataPackage('FUNDOS_CVM-inf_diario_fi_fundos' + key,
                            staging_area)
    dps1[key].create()

for ix in range(0, 10):
    key = str(ix)
    ds1 = INF_DIARIO_FI_MonthlyDataset(
        'FUNDOS_CVM-inf_diario_fi_arquivos_mes'+key, staging_area)
    funds = ds1.get_data()
    for fx in funds:
        dps1[key].save_data(funds[fx], f'{fx}.json', ds1.get_data_files())
    dps1[key].deploy_data(final_staging, 'FUNDOS_CVM-inf_diario_fi')

# for dx in dps:
#     dps[dx].pack()
#     dps[dx].upload(f'gs://ks-tmp/FUNDOS_CVM-inf_diario_fi_proc{dx}.zip')
