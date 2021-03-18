import subprocess, time, multiprocessing

cpu_count = multiprocessing.cpu_count()
script_path = "rabbitmq-GLS.py"
end_number = "5000000000"

for i in range(cpu_count-1):
    subprocess.Popen(["python", script_path, "-e", end_number, "-p", "2"])
subprocess.Popen(["python", script_path, "-e", end_number, "-p", "1"])
time.sleep(2)
subprocess.Popen(["python", script_path, "-e", end_number, "-p", "0"])