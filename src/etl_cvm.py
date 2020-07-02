
import os

from kyd.etl import StagingArea, DataPackage, SeriesDataset
from kyd.etl.b3 import CDI_CETIP_RawDataset
from kyd.etl.cvm import INF_DIARIO_FI_RawDataset

final_staging = StagingArea(r'-----')
final_staging.create()

staging_area = StagingArea(r'-----')
staging_area.create()

ds_cdi_cetip = CDI_CETIP_RawDataset('CDI-cetip_rawdata', staging_area)
ds_cdi_cetip.package.create(dict(source_bucket='ks-rawdata-cetip',
                                 source_prefix='MediaCDI'))
ds_cdi_cetip.package.load()
ds_cdi_cetip.package.update()

ds_cdi_b3 = CDI_B3_RawDataset('CDI-b3_rawdata', staging_area)
ds_cdi_b3.package.create(dict(source_bucket='ks-rawdata-b3',
                              source_prefix='TaxaCDI'))
ds_cdi_b3.package.load()
ds_cdi_b3.package.update()

ds1 = SeriesDataset('CDI-series', staging_area)
ds1.package.create()
ds1.save_data(data=ds.get_data(), processed=ds.get_data_files())
ds1.package.deploy_data(final_staging)
ds1.package.pack()
ds1.package.upload('gs://ks-tmp/CDI-series.zip')
