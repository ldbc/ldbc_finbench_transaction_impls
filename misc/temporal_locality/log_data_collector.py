import sys

f = open(sys.argv[1], "r")
lines = f.readlines()

warmup_size = 500
total = 0
ite = 0
count = 0
for line in lines:
    if line.startswith("iterationTime"):
        count += 1
        if count > warmup_size:
            split_list =line.strip().split(", ")
            iteration_time = float(split_list[0].split(": ")[1])
            execution_time = float(split_list[1].split(": ")[1])
            ite += iteration_time
            total += execution_time
count -= warmup_size
print("[ITERATION]: %f" %(ite / count))
print("[EXECUTION]: %f" %(total / count))