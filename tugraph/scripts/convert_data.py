from sys import argv
import os
import time
import datetime

def to_timestamp(date):
    dt = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S.%f")
    return str(int(time.mktime(dt.timetuple()) * 1000 + (dt.microsecond / 1000)))

def convert_file(in_file, out_file):
    out = open(out_file, 'w')
    with open(in_file, 'r') as f:
        ts_pos = []
        for i, line in enumerate(f):
            fields = line.strip('\n').split('|')
            if i == 0:
                for j, item in enumerate(fields):
                    if 'createTime' in item:
                        ts_pos.append(j)
                out.write(line)
                continue
            for j in ts_pos:
                len_diff = len('2021-04-04 03:19:09.026') - len(fields[j])
                if len_diff == 4:
                    fields[j] += '.'
                    len_diff -= 1
                fields[j] += ('0' * len_diff)
                fields[j] = to_timestamp(fields[j])
            res = '|'.join(fields)
            out.write(res)
            out.write("\n")

if __name__ == '__main__':
    input_dir = argv[1]
    output_dir = argv[2]
    files = os.listdir(input_dir)
    for file in files:
        print(file)
        convert_file('%s/%s' % (input_dir, file), '%s/%s' % (output_dir, file))
