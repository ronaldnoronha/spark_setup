import psutil

def get_pid(process_name):
    for proc in psutil.process_iter():
        print(proc.name())
        if proc.name() == process_name:
            return proc.pid

process_name = "Google Chrome"
print(get_pid(process_name))
