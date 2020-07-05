
import os

from kyd.etl import StagingArea, DataPackage, SeriesDataset
from kyd.etl.b3 import CDI_CETIP_RawDataset
from kyd.etl.cvm import INF_DIARIO_FI_RawDataset

final_staging = StagingArea(r'/opt/etl/final_staging')
final_staging.create()

staging_area = StagingArea(r'/opt/etl/staging_area')
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

# ds_fi_inf_diario = INF_DIARIO_FI_RawDataset('FUNDOS_CVM-inf_diario_fi_rawdata',
#                                             staging_area)
# ds_fi_inf_diario.package.create(dict(source_bucket='ks-tmp',
#                                      source_prefix='INF-DIARIO-FI'))
# ds_fi_inf_diario.package.load()
# ds_fi_inf_diario.package.update()