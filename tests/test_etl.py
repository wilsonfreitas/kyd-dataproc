
import os
import context

from kyd.etl import StagingArea, DataPackage
from kyd.etl.b3 import CDI_CETIP_RawDataset
from kyd.etl.cvm import INF_DIARIO_FI_RawDataset

final_staging = StagingArea(r'C:\Users\wilso\Dev\google-cloud\kyd-dataproc'
                            r'\tests\final-staging')
final_staging.drop()
final_staging.create()

staging_area = StagingArea(r'C:\Users\wilso\Dev\google-cloud\kyd-dataproc'
                           r'\tests\staging')
staging_area.drop()
staging_area.create()

dp = DataPackage('CDI-cetipdata', staging_area)
dp.create({
    'source_bucket': 'ks-tmp',
    'source_prefix': 'CETIP-CDI'
})
dp.pack()
dp.upload('gs://ks-tmp/CDI-cetipdata.zip')

dp = DataPackage('CDI-cetipdata', staging_area)
dp.download('gs://ks-tmp/CDI-cetipdata.zip')
dp.unpack()
dp.load()
dp.update()

dp = DataPackage('CDI-cetipdata', staging_area)
dp.load()
dp.update()
dp.pack()
dp.upload('gs://ks-tmp/CDI-cetipdata.zip')

ds = CDI_CETIP_RawDataset('CDI-cetipdata', staging_area)
print(ds.get_data())
print(ds.get_data_files())

dp = DataPackage('CDI-series', staging_area)
dp.create()
dp.save_data(data=ds.get_data(), filename='series.json',
             processed=ds.get_data_files())
dp.deploy_data(final_staging)
dp.pack()
dp.upload('gs://ks-tmp/CDI-series.zip')

# ks-tmp/INF-DIARIO-FI

dp = DataPackage('FUNDOS_CVM-inf_diario_fi', staging_area)
dp.create({
    'source_bucket': 'ks-tmp',
    'source_prefix': 'INF-DIARIO-FI'
})
dp.load()
dp.update()
dp.pack()
dp.upload('gs://ks-tmp/FUNDOS_CVM-inf_diario_fi.zip')

ds = INF_DIARIO_FI_RawDataset('FUNDOS_CVM-inf_diario_fi', staging_area)
print(ds.get_data_files())

dps = {}
for ix in range(0, 10):
    key = str(ix)
    dps[key] = DataPackage('FUNDOS_CVM-inf_diario_fi_proc' + key, staging_area)
    dps[key].create()

funds = ds.get_data()
for dx in funds:
    for fx in funds[dx]:
        key = fx[-1]
        dps[key].save_data(funds[dx][fx], os.path.join(fx, f'{dx}.json'),
                           ds.get_data_files())

for dx in dps:
    dps[dx].pack()
    dps[dx].upload(f'gs://ks-tmp/FUNDOS_CVM-inf_diario_fi_proc{dx}.zip')
