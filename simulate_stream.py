import os
import csv
import random
import time
import shutil
import argparse

STREAM_DIRECTORY = '/tmp/power_stream'
SCRIPT_DIRECTORY = os.path.dirname(os.path.abspath(__file__))
SAMPLE_DATA_PATH = os.path.join(SCRIPT_DIRECTORY, 'data', 'uci_power_sample.csv')

RESIDENTIAL_METER_IDS = [f'M{i:03d}' for i in range(1, 9)]

# boost added to residential readings during anomaly batches
ANOMALY_BOOST_KW = 4.0

batch_number = 0


def load_sample_data():
    rows = []
    with open(SAMPLE_DATA_PATH, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def write_batch(rows, spike_residential=False):
    global batch_number
    batch_number += 1

    file_path = os.path.join(STREAM_DIRECTORY, f'batch_{batch_number:04d}.csv')

    output_rows = []
    for row in rows:
        meter_id = row['meter_id']
        power_kw = float(row['global_active_power'])

        if spike_residential and meter_id in RESIDENTIAL_METER_IDS:
            # push residential above industrial range
            power_kw = round(power_kw + ANOMALY_BOOST_KW, 3)
        else:
            power_kw = round(power_kw, 3)

        output_rows.append([row['event_time'], meter_id, power_kw])

    with open(file_path, 'w', newline='') as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(['event_time', 'meter_id', 'global_active_power'])
        writer.writerows(output_rows)

    label = 'RESIDENTIAL SPIKE' if spike_residential else 'normal'
    print(f'[batch {batch_number:04d}] {len(output_rows)} readings ({label})')


def main():
    parser = argparse.ArgumentParser(
        description='Stream UCI power data as CSV batches')
    parser.add_argument('--rows-per-batch', type=int, default=200,
                        help='readings per batch file')
    parser.add_argument('--interval', type=float, default=3.0,
                        help='seconds between batches')
    parser.add_argument('--anomaly-ratio', type=float, default=0.3,
                        help='fraction of batches with residential spikes')
    parser.add_argument('--max-batches', type=int, default=50,
                        help='stop after this many batches (0 = use all data)')
    args = parser.parse_args()

    if not os.path.exists(SAMPLE_DATA_PATH):
        print(f'Error: {SAMPLE_DATA_PATH} not found.')
        print('Place the UCI sample CSV in the data/ directory.')
        raise SystemExit(1)

    all_rows = load_sample_data()
    total_batches = len(all_rows) // args.rows_per_batch
    if args.max_batches > 0:
        total_batches = min(total_batches, args.max_batches)

    if os.path.exists(STREAM_DIRECTORY):
        shutil.rmtree(STREAM_DIRECTORY)
    os.makedirs(STREAM_DIRECTORY)

    print(f'Source: {SAMPLE_DATA_PATH} ({len(all_rows)} rows)')
    print(f'{total_batches} batches x {args.rows_per_batch} rows, '
          f'interval={args.interval}s, anomaly ratio={args.anomaly_ratio}\n')

    for i in range(total_batches):
        start = i * args.rows_per_batch
        end = start + args.rows_per_batch
        batch_rows = all_rows[start:end]

        should_spike = random.random() < args.anomaly_ratio
        write_batch(batch_rows, spike_residential=should_spike)

        if i < total_batches - 1:
            time.sleep(args.interval)

    print('\nDone.')


if __name__ == '__main__':
    main()
