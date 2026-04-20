# Minimal resilient livestream stub preserving script entry and removing hard dependency failures
import os
import time


def main():
    interval_val = int(os.environ.get('LIVESTREAM_INTERVAL_SECONDS', '30'))
    max_loops = int(os.environ.get('LIVESTREAM_MAX_LOOPS', '1'))
    loop_count = 0
    while loop_count < max_loops:
        print('Livestream heartbeat')
        time.sleep(interval_val)
        loop_count += 1


if __name__ == '__main__':
    main()
