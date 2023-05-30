from pyapp.ewon_translate import start
from logging import warning

warning('run.py called')
if __name__ == '__main__':
    warning('STARTING NODE')
    start()