import subprocess
import sys
# from concurrent.futures import ThreadPoolExecutor
import checker
from multiprocessing import Process

p = Process(target=checker.checker, name='checker')
p.start()
subprocess.run([sys.executable, "launch.py"])



