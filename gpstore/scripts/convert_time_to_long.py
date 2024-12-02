# Detect date / datetime fields, convert to UNIX timestamp
import os
datetime_dir = "content_datetime/"
# datetime_dir = "content_datetime_toy/"
long_dir = "content_long/"
if not os.path.exists(long_dir):
    os.makedirs(long_dir)
import os
from datetime import datetime
formats=["%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]
filenames = os.listdir(datetime_dir)
for filename in filenames:
    with open(datetime_dir + filename, "r") as f, open(long_dir + filename, "w") as fout:
        lines = f.readlines()
        i = 0
        numlines = len(lines)
        while i < numlines:
            line_ls = lines[i].split('|')
            if len(line_ls) != 0:
                convert_idx_2_format = {}
                for i in range(len(line_ls)):
                    # Detect automatically using Python datetime
                    for format in formats:
                        try:
                            datetime.strptime(line_ls[i], format)
                            convert_idx_2_format[i] = format
                            break
                        except ValueError:
                            pass
                break
        print(filename, convert_idx_2_format)
    
        for line in lines:
            line_ls = line.split('|')
            if len(line_ls) == 0:
                continue
            write_line = ""
            for i in range(len(line_ls)):
                if i > 0:
                    write_line += "|"
                if i in convert_idx_2_format:
                    try:
                        ts = datetime.strptime(line_ls[i], convert_idx_2_format[i]).timestamp()
                    except ValueError:
                        if convert_idx_2_format[i] == "%Y-%m-%d %H:%M:%S.%f":
                            try:
                                ts = datetime.strptime(line_ls[i], "%Y-%m-%d %H:%M:%S").timestamp()
                            except ValueError:
                                pass
                    # if ts.is_integer():
                    #     write_line += str(int(ts))
                    # else:
                    #     write_line += str(float(ts))
                    write_line += str(int((ts + 28800) * 1000.))    # UTC+8
                else:
                    write_line += line_ls[i]
            fout.write(write_line)