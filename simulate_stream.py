import os
import csv
import random
import time
import shutil
import argparse
from datetime import datetime, timedelta

STREAM_DIRECTORY = '/tmp/power_stream'

# baseline power draw (kW) per meter; M001-M008 are residential, M009-M016 industrial
METER_BASELINES_KW = {
    'M001': 1.2, 'M002': 0.8, 'M003': 1.5, 'M004': 1.0,
    'M005': 0.9, 'M006': 1.3, 'M007': 1.1, 'M008': 0.7,
    'M009': 4.5, 'M010': 5.2, 'M011': 6.0, 'M012': 4.8,
    'M013': 5.5, 'M014': 6.3, 'M015': 5.0, 'M016': 5.8,
}

RESIDENTIAL_METER_IDS = [f'M{i:03d}' for i in range(1, 9)]

batch_number = 0


def write_power_reading_batch(events_per_batch=100, spike_residential=False):
    global batch_number
    batch_number += 1

    file_path = os.path.join(STREAM_DIRECTORY, f'batch_{batch_number:04d}.csv')
    base_timestamp = datetime.now()
    all_meter_ids = list(METER_BASELINES_KW.keys())

    rows = []
    for i in range(events_per_batch):
        meter_id = random.choice(all_meter_ids)
        baseline_kw = METER_BASELINES_KW[meter_id]

        if spike_residential and meter_id in RESIDENTIAL_METER_IDS:
            # push residential readings well above typical industrial range
            active_power_kw = round(baseline_kw + random.uniform(5.0, 8.0), 3)
        else:
            active_power_kw = round(baseline_kw + random.uniform(-0.3, 0.5), 3)

        event_time = base_timestamp + timedelta(seconds=i)
        rows.append([
            event_time.strftime('%Y-%m-%d %H:%M:%S'),
            meter_id,
            active_power_kw,
        ])

    with open(file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['event_time', 'meter_id', 'global_active_power'])
        writer.writerows(rows)

    label = 'RESIDENTIAL SPIKE' if spike_residential else 'normal'
    print(f'[batch {batch_number:04d}] {events_per_batch} readings ({label})')


def main():
    parser = argparse.ArgumentParser(description='Simulate smart meter streaming data')
    parser.add_argument('--batches', type=int, default=30,
                        help='number of CSV batches to write')
    parser.add_argument('--events-per-batch', type=int, default=100,
                        help='meter readings per batch')
    parser.add_argument('--interval', type=float, default=3.0,
                        help='seconds between batches')
    parser.add_argument('--anomaly-ratio', type=float, default=0.3,
                        help='fraction of batches with residential spikes')
    args = parser.parse_args()

    if os.path.exists(STREAM_DIRECTORY):
        shutil.rmtree(STREAM_DIRECTORY)
    os.makedirs(STREAM_DIRECTORY)

    print(f'Streaming to {STREAM_DIRECTORY}')
    print(f'{args.batches} batches x {args.events_per_batch} events, '
          f'interval={args.interval}s, anomaly ratio={args.anomaly_ratio}\n')

    for i in range(args.batches):
        should_spike = random.random() < args.anomaly_ratio
        write_power_reading_batch(args.events_per_batch, spike_residential=should_spike)
        if i < args.batches - 1:
            time.sleep(args.interval)

    print('\nDone.')


if __name__ == '__main__':
    main()
