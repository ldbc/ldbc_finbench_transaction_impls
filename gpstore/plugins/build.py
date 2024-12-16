qtype2num = {"tcr": 12, "tsr": 6, "tw": 19, "trw": 3}
import os
import subprocess
path_pref = "/gpstore/plugins/"   # Default absolute path for Docker
dependencies_prefix = ["../Value/", "../PQuery/", "../PQuery/", "../PQuery/", "../Database/", "../Database/"]
dependencies = ["Expression", "Node", "PProcedure", "PTempResult", "Database", "database_build_methods"]
for i in range(len(dependencies)):
    dep = dependencies[i]
    pref = dependencies_prefix[i]
    command = ['g++',
    '-std=c++17',
    '-I..',
    '-I../tools/log4cplus/include',
    '-I../tools/antlr4-cpp-runtime-4/runtime/src',
    '-fPIC',
    pref + '/' + dep + '.cpp',
    '-shared',
    '-o',
    dep + '.so']
    if dep == "database_build_methods":
        command += [path_pref + "/Database.so"]
    elif dep == "PTempResult":
        command += [path_pref + "/Expression.so"]
    elif dep == "Database":
        command += [path_pref + "/PProcedure.so"]

    subprocess.run(command, check=True)
dep_suffix = []
for dep in dependencies:
    dep_suffix.append(path_pref + f"/{dep}.so")
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