import argparse
import sys

from prep_pavenet.core.core import Core


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file path", required=True)
    args = parser.parse_args()
    core = Core(args.config)
    core.prepare_scenario()


if __name__ == "__main__":
    sys.exit(main())
