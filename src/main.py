from logging.config import fileConfig
from os import path
from src.manager import Manager


def main():
    log_conf_path = path.join(path.dirname(path.abspath(__file__)), 'shared/logging_config.ini')
    fileConfig(log_conf_path, disable_existing_loggers=False)
    Manager().run()


if __name__ == '__main__':
    main()
