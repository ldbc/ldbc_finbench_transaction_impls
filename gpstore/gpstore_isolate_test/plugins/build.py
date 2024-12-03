qtype2num = {"tcr": 12, "tsr": 6, "tw": 19, "trw": 3}
import os
import subprocess
path_pref = "/home/pangyue/gpstore_isolate_test/"   # Please input absolute path
dependencies = ["Node", "PProcedure"]
for dep in dependencies:
    subprocess.run(['g++',
    '-std=c++17',
    '-I..',
    '-I../tools/log4cplus/include',
    '-I../tools/antlr4-cpp-runtime-4/runtime/src',
    '-fPIC',
    '../PQuery/' + dep + '.cpp',
    '-shared',
    '-o',
    dep + '.so'])
dep_suffix = []
for dep in dependencies:
    dep_suffix.append(path_pref + f"/plugins/{dep}.so")
for qtype in qtype2num:
    for i in range(1, qtype2num[qtype] + 1):
        src_file = f"{qtype}{i}.cpp"
        output_file = f"{qtype}{i}.so"
        command = [
            "g++", "-std=c++17",
            "-I..", "-I../tools/log4cplus/include",
            "-I../tools/antlr4-cpp-runtime-4/runtime/src",
            "-fPIC", src_file, "-shared", "-o", output_file
        ]
        command += dep_suffix
        if os.path.exists(src_file):
            print(f"Building {src_file}...")
            try:
                # Execute the compilation command
                subprocess.run(command, check=True)
            except subprocess.CalledProcessError as e:
                print(f"Error compiling {src_file}: {e}")