import time
from elama_project.working_code import main
def timer(f):
    start_time=time.time()
    f()
    elapsed_time=time.time()-start_time
    with open('time.txt','a') as fl:
        print('Time of running function {} is {}    {}'.format(f.__name__,elapsed_time,time.asctime()),file=fl)
timer(main)
