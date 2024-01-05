from __future__ import annotations

import argparse
import sys
import time

from prep_pavenet.core.core import Core


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file path", required=True)
    args = parser.parse_args()
    core = Core(args.config)
    # calculate execution time
    exec_start = time.time()
    core.prepare_scenario()
    exec_end = time.time()
    print(f"Execution time: {exec_end - exec_start} seconds")


if __name__ == "__main__":
    sys.exit(main())
