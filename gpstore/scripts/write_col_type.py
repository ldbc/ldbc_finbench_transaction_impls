# Interactively ask for column type and output to file
import os
header_dir = "orig_headers/"
intermediate_dir = "intermediate/"
new_header_dir = "headers/"
if not os.path.exists(new_header_dir):
    os.makedirs(new_header_dir)
filenames = os.listdir(header_dir)
datatypes = ["ID", "START_ID", "END_ID", "STRING", "FLOAT", "INT", "LONG", "BOOLEAN", "STRING_LIST"]
for filename in filenames:
    with open(intermediate_dir + filename, "r") as f, open(new_header_dir + filename, "w") as fout:
        node_name = ""
        src_name = ""
        dst_name = ""
        write_line = ""
        lines = f.readlines()
        for i in range(len(lines)):
            line = lines[i]
            line_ls = line.split()
            if len(line_ls) == 0:
                break
            if i > 0:
                write_line += "|"
            if i == 0 and line_ls[1] == "ID":
                write_line += "id:ID"
            else:
                write_line += line_ls[0] + ":" + line_ls[1]
            if i == 0:
                if line_ls[1] == "ID":
                    node_name = filename[:filename.find(".csv")]
                else:
                    rel_name_start_idx = 1
                    while filename[rel_name_start_idx] >= 'a' and filename[rel_name_start_idx] <= 'z' and rel_name_start_idx < len(filename):
                        rel_name_start_idx += 1
                    rel_name_end_idx = rel_name_start_idx + 1
                    while filename[rel_name_end_idx] >= 'a' and filename[rel_name_end_idx] <= 'z' and rel_name_end_idx < len(filename):
                        rel_name_end_idx += 1
                    src_name = filename[:rel_name_start_idx]
                    dst_name = filename[rel_name_end_idx:filename.find(".csv")]
                if node_name != "":
                    write_line += "(" + node_name + ")"
                else:
                    write_line += "(" + src_name + ")"
            elif i == 1:
                if dst_name != "":
                    write_line += "(" + dst_name + ")"
        fout.write(write_line + '\n')