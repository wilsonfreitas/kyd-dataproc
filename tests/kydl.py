
# kydl = kyd launcher

config = [
    {
        'input_bucket': 'ks-tmp',
        'output_bucket': 'ks-tmp',
        'input_prefix': 'CETIP-CDI',
        'symbol': 'CDI',
        'name': 'cetipdata',
        'type': 'one-to-one'
    },
    {
        'input_bucket': 'ks-tmp',
        'symbol': 'CDI',
        'source_name': 'cetipdata',
        'name': 'series',
        'type': 'series_composer'
    },
    {
        'input_bucket': 'ks-rawdata-cetip',
        'output_bucket': 'ks-objects',
        'input_prefix': 'MediaCDI',
        'symbol': 'CDI',
        'name': 'cetipdata',
        'input_regex': r'.*(\d{4}\d{2}\d{2}).*$',
        'type': 'one-to-one'
    },
    {
        'input_bucket': 'ks-rawdata-cetip',
        'output_bucket': 'ks-objects',
        'input_prefix': 'IndiceDI',
        'symbol': 'IDI',
        'name': 'cetipdata',
        'input_regex': r'.*(\d{4}\d{2}\d{2}).*$',
        'type': 'one-to-one'
    },
    {
        'input_bucket': 'ks-objects',
        'symbol': 'IDI',
        'source_name': 'cetipdata',
        'name': 'series',
        'type': 'series_composer'
    },
    {
        'input_bucket': 'ks-objects',
        'symbol': 'CDI',
        'source_name': 'cetipdata',
        'name': 'series',
        'type': 'series_composer'
    }
]

from kyd.handlers.cvm import INF_DIARIO_FI_OneToManyHandler
from kyd.handlers.b3 import CETIP_CDI_OneToOneHandler
from kyd.handlers import SeriesComposer

ph = INF_DIARIO_FI_OneToManyHandler({
    'input_bucket': 'ks-tmp',
    'output_bucket': 'ks-tmp',
    'input_prefix': 'INF-DIARIO-FI',
    'symbol': 'CVM-INF-DIARIO',
    'name': 'cvmdata',
    'type': 'one-to-many'
})
ph.execute()

# ph = CETIP_CDI_OneToOneHandler({
#     'input_bucket': 'ks-tmp',
#     'output_bucket': 'ks-tmp',
#     'input_prefix': 'CETIP-CDI',
#     'symbol': 'CDI',
#     'name': 'cetipdata',
#     'type': 'one-to-one'
# })
# ph.execute()
# sc = SeriesComposer(config[1])
# sc.execute()

