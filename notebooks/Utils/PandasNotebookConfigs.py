import pandas as pd


def config():
    # PANDAS CONFIGS
    # Disable scientific notation
    print(f'Ajustando display.float.format para %.2f')
    pd.set_option('display.float_format', lambda x: '%.2f' % x)