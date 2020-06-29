
import os
import context

from kyd.etl import StagingArea, DataPackage, SeriesDataset
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

ds = CDI_CETIP_RawDataset('CDI-cetipdata', staging_area)
ds.package.create({
    'source_bucket': 'ks-tmp',
    'source_prefix': 'CETIP-CDI'
})
ds.package.load()
ds.package.update()
ds.package.pack()
ds.package.upload('gs://ks-tmp/CDI-cetipdata.zip')

print(ds.get_data())
print(ds.get_data_files())

ds1 = SeriesDataset('CDI-series', staging_area)
ds1.package.create()
ds1.save_data(data=ds.get_data(), processed=ds.get_data_files())
ds1.package.deploy_data(final_staging)
ds1.package.pack()
ds1.package.upload('gs://ks-tmp/CDI-series.zip')
