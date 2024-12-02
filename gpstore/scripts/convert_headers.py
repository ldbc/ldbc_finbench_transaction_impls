import os
data_dir = "content_datetime/"
header_dir = "orig_headers/"
# Create header_dir
if not os.path.exists(header_dir):
    os.makedirs(header_dir)
filenames = os.listdir(data_dir)
for filename in filenames:
    if filename.endswith(".csv"):
        header = None
        with open(data_dir + filename, "r") as f:
            header = f.readline()
        with open(header_dir + filename, "w") as f:
            f.write(header)
# Use sed to remove first line from all content files
# os.system("sed -i 1d content/*.csv")