# coding=utf-8
'''
Bootstraps two entities data reconciliation process

Build package.zip
    >>> bash recon/build.sh

Run job

    >>>    spark-submit --master yarn \
    >>>    --py-files recon/package.zip \
    >>>    recon/run.py -src sandbox_apolyakov.recon_source -dst sandbox_apolyakov.recon_dest -j recon/table_name_recon_config.json

'''
import argparse
from src.recon import Recon


def get_args():
    '''
    Getting all passed arguments

    '''
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-src',
        action='store',
        help='Reconciliation source table name',
        required=True
    )
    parser.add_argument(
        '-dst',
        action='store',
        help='Reconciliation destination table name',
        required=True
    )
    parser.add_argument(
        '-j',
        help='JSON Reconciliation config file',
        required=False
    )
    args = {key: value for key, value in vars(
        parser.parse_args()).items() if value}

    return args

if __name__ == '__main__':
    recon = Recon(**get_args())
    recon.run()
