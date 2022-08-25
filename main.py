from multiprocessing import Process
import random
import time
import datetime
import queue
# import pyRAPL
import math
import numpy
import macro_define
import request_define
import ssd
import re
import high_freq_proc

'''
pyRAPL.setup(devices=[pyRAPL.Device.GPU], socket_ids=[1])

used for power analysis, currently not working for AMD chipset
'''

array = [854, 471, 856, 79]
cnter_test = 0

ssd_process_time = 0.016  # assume process time is constant number, unit: ms
counter = 1
# queue definition
incoming_queue = queue.Queue()  # query from clients side
sending_out_queue = queue.PriorityQueue()
long_payload_queue = queue.Queue()  # query from clients side with long video request
high_freq_op_queue = queue.Queue()  # query from clients side with high frequency video operation
normal_query_queue = queue.Queue()  # query from clients side with short video request
iskeva_and_queue = queue.Queue()  # query in ISKEVA for AND queue
iskeva_or_queue = queue.Queue()  # query in ISKEVA for OR queue
iskeva_exist_queue = queue.Queue()  # query in ISKEVA for EXIST queue
iskeva_not_queue = queue.Queue()  # query in ISKEVA for NOT queue
iskeva_cnt_queue = queue.Queue()  # query in ISKEVA for CNT queue
iskeva_write_queue = queue.Queue()
iskeva_store_and_queue = queue.Queue()  # query in ISKEVA for AND queue
iskeva_store_or_queue = queue.Queue()  # query in ISKEVA for OR queue
iskeva_store_exist_queue = queue.Queue()  # query in ISKEVA for EXIST queue
iskeva_store_not_queue = queue.Queue()  # query in ISKEVA for NOT queue
iskeva_store_cnt_queue = queue.Queue()  # query in ISKEVA for CNT queue
datacenter_request_queue = queue.Queue()  # query from datacenter to do prediction process

#  assume 10 video has been store into memory blade
#stored_video = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
stored_video = [1, 2]
#  available memory blade(SSDs) will be store in here
ssd_container = []
ssd_busy_end_time = [0] * macro_define.SSD_NUMBER
cpu_busy_end_time = [0] * macro_define.CPU_NUMBER
# flag used for control priority between long_payload_queue and normal_query_queue
# if pre_proc_flag = 0, normal_query_queue has higher priority
# if pre_proc_flag = 1, long_payload_queue has higher priority
#  currently not sensible
pre_proc_flag = 0

# when datacenter request queue is not empty, the system will process query from datacenter, whenever incoming queue
# is not empty, datacenter_request_flag will = 0, until system processing all incoming queue
datacenter_request_flag = 1

#  supported opreation from ISKEVA
operation_list = ["AND", "OR", "EXIST", "NOT", "CNT", "WRITE"]
store_operation_list = ["STORE_AND", "STORE_OR", "STORE_EXIST", "STORE_NOT", "STORE_CNT"]
# store_operation_list = ["STORE_AND"]
# operation_list = ["AND"]

#  assumed user requested feature list
feature_list = ["cat", "bird", "dog", "rabbit", "tiger", "fish"]
# used for check operation purpose
query_check_board = []
op_count_dict = {}  # count same operation frequency for pre-process
flipped_op_count_dict = {}  # intermediate data structure for pre-process
high_frq_dict = {}  # intermediate data structure for pre-process

finish_list = []  # request used for analysis
shortcut_finish_list = []
time_record = []  # used for record finish time
tmp_board = []
user_response_time = []
# for logn feature
write_feature_list = []


def generate_user_request(i):
    global cnter_test
    user_request = request_define.userrequest_type()
    user_request.length = random.randint(1, 1000)  # random.randint(1, 999)
    # cnter_test += 1
    user_request.video_id = random.randint(1, 10)
    # user_request.video_id = 1
    # user_request.features = random.sample(feature_list, random.randint(2, 6))
    # user_request.features = ["cat", "tiger"]
    user_request.generate_time = time.time()
    user_request.features = random.sample(feature_list, 2)
    user_request.operation = random.choice(operation_list)
    user_request.features_size1 = random.randint(10, 500)  # KB
    user_request.features_size2 = random.randint(10, 500)  # KB
    if user_request.features_size1 > macro_define.FEATURE_SIZE_THRESHOLD or user_request.features_size2 > macro_define.FEATURE_SIZE_THRESHOLD:
        user_request.max_feature_size = 1
    else:
        user_request.max_feature_size = 0
    user_request.finish_time = 0
    if i == 0:
        user_request.arrival_time = 0
    else:
        last_request = incoming_list[i - 1]
        try:
            user_request.arrival_time = last_request.arrival_time + numpy.random.exponential(scale=0.01, size=None)
        except OverflowError:
            user_request.arrival_time = last_request.arrival_time + float("inf")
    attrs = vars(user_request)
    #print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
    return user_request


# SSD request generator
def ssd_generator():
    ssd_strut = ssd.ssds()
    ssd_strut.access_latency = 9  # 9ms
    ssd_strut.state = "NOT_BUSY"
    ssd_strut.capacity = 1000  # GB

    attrs_ssd = vars(ssd_strut)
    #print(', '.join("%s: %s" % item for item in attrs_ssd.items()))  # #print generated items
    return ssd_strut


incoming_list = []


#  initialize numbers of request and send to data center
def driver():
    '''    for i in range(0, macro_define.REQUEST_NUMBER):
        new_request = generate_user_request(i)
        incoming_queue.put(new_request)
        incoming_list.append(new_request)'''
    with open("C://Users//pingyi//Desktop//trace2.txt", "r") as f:
        for i in range(0, macro_define.REQUEST_NUMBER):
            line = f.readline()
            length = re.findall(r".*length: (.*?) ", line)
            length = length[0]

            video_id = re.findall(r".*video_id: (.*?) ", line)
            video_id = video_id[0]

            features = re.findall(r".*features: (.*?) f", line)
            features = features[0]

            features_size1 = re.findall(r".*features_size1: (.*?) ", line)
            features_size1 = features_size1[0]

            features_size2 = re.findall(r".*features_size2: (.*?) ", line)
            features_size2 = features_size2[0]

            operation = re.findall(r".*operation: (.*?) ", line)
            operation = operation[0]

            arrival_time = re.findall(r".*arrival_time: (.*?) ", line)
            arrival_time = arrival_time[0]

            finish_time = re.findall(r".*finish_time: (.*?) ", line)
            finish_time = finish_time[0]

            max_feature_size = re.findall(r".*max_feature_size: (.*?) ", line)
            max_feature_size = max_feature_size[0]

            priority = re.findall(r".*priority: (\d)", line)
            priority = priority[0]

            new_request = request_define.userrequest_type()
            new_request.length = length
            new_request.video_id = video_id
            new_request.features = features
            new_request.features_size1 = features_size1
            new_request.features_size2 = features_size2
            new_request.operation = str(operation)
            new_request.arrival_time = float(arrival_time)
            new_request.finish_time = float(finish_time)
            new_request.max_feature_size = max_feature_size
            new_request.priority = int(priority)
            new_request.generate_time = time.time()
            attrs = vars(new_request)
            #print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
            incoming_queue.put(new_request)
    f.close()

    for i in range(0, macro_define.SSD_NUMBER):
        ssd_container.append(ssd_generator())
    return


def update_ssd_state(finish_time, occupied_ssd_id, stime, request):
    ssd_busy_end_time[occupied_ssd_id - 1] = finish_time


resultList = []
incoming_query_list = []


def user_request_comparator(i, pre_proc_request):
    if  i.__dict__['video_id'] == \
            pre_proc_request.__dict__['video_id'] and i.__dict__['features'] == pre_proc_request.__dict__[
        'features'] and i.__dict__['operation'] == pre_proc_request.__dict__['operation'] and i.__dict__['priority'] == \
            pre_proc_request.__dict__['priority']:
        return True
    return False


def update_query_check_board(pre_proc_request):
    #print("***************")
    #print_query_check_board()
    atr = vars(pre_proc_request)
    #print(', '.join("%s: %s" % item for item in atr.items()))
    #print("****************")
    if len(query_check_board) == 0:
        query_check_board.append(pre_proc_request)
        return 0
    else:
        for i in query_check_board:
            if user_request_comparator(i, pre_proc_request):
                return 1
        query_check_board.append(pre_proc_request)
    return 0


def update_incoming_check_board(pre_proc_request):
    #print("***************")
    ##print_query_check_board()
    atr = vars(pre_proc_request)
    #print(', '.join("%s: %s" % item for item in atr.items()))
    #print("****************")
    if len(query_check_board) == 0:
        incoming_query_list.append(pre_proc_request)
        return 0
    else:
        for i in query_check_board:
            if user_request_comparator(i, pre_proc_request):
                return 1
        incoming_query_list.append(pre_proc_request)
    return 0


def print_query_check_board():
    for i in query_check_board:
        attrs = vars(i)
        # #print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items


# function simulate ISKEVA behavior



def ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container):
    if ssd_container[occupied_ssd_id - 1].state == "BUSY":
        etime_before_ssd_proc = time.time()
        server_etime_before_ssd_proc = etime_before_ssd_proc - stime
        if (request.arrival_time + server_etime_before_ssd_proc) < ssd_busy_end_time[occupied_ssd_id - 1]:
            if request.priority == macro_define.HIGH_PRIORITY:
                finish_time = max(request.arrival_time, ssd_busy_end_time[counter - 1]) + (etime_before_ssd_proc - stime) + ssd_program_latency + internet_transfer_time
                #print("finish time is: ")
                #print("11111")
                #print(finish_time)
                request.finish_time = finish_time
                query_process_time = (etime_before_ssd_proc - stime) + ssd_program_latency + internet_transfer_time + ssd_busy_end_time[counter - 1]
                query_process_time = ssd_update_cpu_time(query_process_time, request)
                query_process_time = user_response_time.append(query_process_time)
                update_ssd_state(finish_time, occupied_ssd_id, stime, request)
            else:
                #print(etime_before_ssd_proc)
                #print(request.arrival_time)
                #print(float(etime_before_ssd_proc - request.arrival_time))
                #print("22222")
                finish_time = float(etime_before_ssd_proc - request.arrival_time) + ssd_program_latency + \
                              ssd_busy_end_time[counter - 1] + internet_transfer_time
                request.finish_time = finish_time
                query_process_time = ssd_update_cpu_time(finish_time, request)
                user_response_time.append(query_process_time)
                update_ssd_state(finish_time, occupied_ssd_id, request.arrival_time, request.arrival_time)
        else:
            if request.priority == macro_define.HIGH_PRIORITY:
                #print("33333")
                finish_time = request.arrival_time + (etime_before_ssd_proc - stime) + ssd_program_latency + internet_transfer_time
                request.finish_time = finish_time
                query_process_time = (etime_before_ssd_proc - stime) + ssd_program_latency + internet_transfer_time
                #print(etime_before_ssd_proc - stime)
                #print(ssd_program_latency)
                #print(internet_transfer_time)
                #print("query_process_time is: ")
                #print(query_process_time)
                shortcut_finish_list.append(query_process_time)
                query_process_time = ssd_update_cpu_time(query_process_time, request)
                user_response_time.append(query_process_time)
                update_ssd_state(finish_time, occupied_ssd_id, stime, request)
            else:
                #print("44444")
                #print(etime_before_ssd_proc)
                #print(request.arrival_time)
                float(etime_before_ssd_proc - request.arrival_time)
                finish_time = float(etime_before_ssd_proc - request.arrival_time) + ssd_program_latency + internet_transfer_time
                #print("finish time is: ")
                #print(finish_time)
                request.finish_time = finish_time
                query_process_time = ssd_update_cpu_time(finish_time, request)
                user_response_time.append(query_process_time)
                update_ssd_state(finish_time, occupied_ssd_id, request.arrival_time, request.arrival_time)
        finish_list.append(request)
    return


def iskeva_and(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_and_queue.put(request)  # fetch from data center queue
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_or(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_or_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_exist(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_exist_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return

def iskeva_not(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_not_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_cnt(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_cnt_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_write(request, stime, counter):
    # ------------------------------------
    # occupied_ssd_id = random.randint(1, 6)  # random select SSD
    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_write_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_store_and(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_store_and_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_store_or(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_store_or_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_store_not(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_store_not_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_store_exist(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_store_exist_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def iskeva_store_cnt(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_store_cnt_queue.put(request)  # fetch from data center qu
    ssd_opreations(request, stime, counter, occupied_ssd_id, ssd_container)
    return


def error_handler(request):
    #print("fatal error, invalid op")
    return false


def large_feature_req_pre_process(request):
    if request.max_feature_size == 0:
        return
    else:
        if request.features not in write_feature_list:
            write_feature_list.append(request.features)
            for i in stored_video:
                for j in range((len(store_operation_list))):
                    pre_proc_request = request_define.userrequest_type()
                    pre_proc_request.length = i
                    pre_proc_request.video_id = i
                    pre_proc_request.features = request.features
                    pre_proc_request.operation = store_operation_list[j]
                    pre_proc_request.arrival_time = time.time()
                    pre_proc_request.max_feature_size = 1
                    pre_proc_request.features_size1 = abs(random.gauss(300, 150))
                    pre_proc_request.features_size2 = abs(random.gauss(300, 150))
                    pre_proc_request.priority = macro_define.MID_PRIORITY
                    bypass_flag = update_query_check_board(pre_proc_request)
                    if bypass_flag == 1:
                        match_time = time.time()
                        pre_proc_request.finish_time = match_time - pre_proc_request.arrival_time + internet_transfer_time
                        finish_list.append(pre_proc_request.finish_time)
                        attrs = vars(pre_proc_request)
                        #print("long feature query matched")
                        #print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
                    else:
                        #print("sending out long feature query")
                        attrs = vars(pre_proc_request)
                        #print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
                        sending_out_queue.put(pre_proc_request)


def high_op_req_pre_process(request):
    op_count_dict[request.video_id] = request.operation, request.features
    for key, value in op_count_dict.items():
        # value = tuple(value)
        if value not in flipped_op_count_dict:
            flipped_op_count_dict[value] = [key]
        else:
            flipped_op_count_dict[value].append(key)
    del (op_count_dict[request.video_id])
    for key, value in flipped_op_count_dict.items():
        current_count = len(value)
        if current_count > int(macro_define.REQUEST_NUMBER * macro_define.HIGH_FREQ_QUERY_THRESHOLD):
            high_frq_dict[key] = value
            for i in stored_video:
                pre_proc_request = request_define.userrequest_type()
                pre_proc_request.length = i
                pre_proc_request.video_id = i
                pre_proc_request.features = key[1]
                pre_proc_request.operation = key[0]
                pre_proc_request.arrival_time = time.time()
                pre_proc_request.features_size1 = abs(random.gauss(300, 150))
                pre_proc_request.features_size2 = abs(random.gauss(300, 150))
                if pre_proc_request.features_size1 or pre_proc_request.features_size2 > macro_define.FEATURE_SIZE_THRESHOLD:
                    pre_proc_request.max_feature_size = 1
                else:
                    pre_proc_request.max_feature_size = 0
                pre_proc_request.priority = macro_define.MID_PRIORITY
                # bypass_flag = update_query_check_board(pre_proc_request)
                if int(pre_proc_request.length) > 5:
                    bypass_flag = update_query_check_board(pre_proc_request)
                    #print("bypass flag is:" + str(bypass_flag))
                    if bypass_flag == 1:
                        match_time = time.time()
                        pre_proc_request.finish_time = match_time - pre_proc_request.arrival_time + internet_transfer_time
                        finish_list.append(pre_proc_request.finish_time)
                        # finish_list.append(pre_proc_request)
                        tmp_board.append(pre_proc_request)
                    else:
                        sending_out_queue.put(pre_proc_request)
                else:
                    pre_proc_request.priority = macro_define.LOW_PRIORITY
                    bypass_flag = update_query_check_board(pre_proc_request)
                    #print("bypass flag is:" + str(bypass_flag))
                    if bypass_flag == 1:
                        match_time = time.time()
                        pre_proc_request.finish_time = match_time - pre_proc_request.arrival_time + internet_transfer_time
                        finish_list.append(pre_proc_request.finish_time)
                        tmp_board.append(pre_proc_request)
                    else:
                        sending_out_queue.put(pre_proc_request)
                        # update_query_check_board(pre_proc_request)
    if current_count > int(macro_define.REQUEST_NUMBER * macro_define.HIGH_FREQ_QUERY_THRESHOLD):
        del flipped_op_count_dict[key]
    # #print(high_frq_dict[key])
    return


def ssd_program_latency_update(sending_out_request):
    global ssd_program_latency
    global internet_transfer_time
    ssd_program_latency = ((float(sending_out_request.features_size1) + float(sending_out_request.features_size2)) / 16) * 0.005 + 0.005
    if ssd_program_latency < 0:
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!fatal error!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
    #print("calculated SSD latency: " + str(ssd_program_latency))
    if sending_out_request.operation == "AND":
        ssd_program_latency = ssd_program_latency * (1/4.95)
    elif sending_out_request.operation == "OR":
        ssd_program_latency = ssd_program_latency * (1 / 1.4)
    elif sending_out_request.operation == "NOT":
        ssd_program_latency = ssd_program_latency * (1 / 3.49)
    elif sending_out_request.operation == "EXIST":
        ssd_program_latency = ssd_program_latency * (1 / 6.98)
    elif sending_out_request.operation == "CNT":
        ssd_program_latency = ssd_program_latency * (1 / 6.98)
    else:
        ssd_program_latency = ssd_program_latency * 1

    internet_transfer_time = (float(sending_out_request.features_size1) + float(sending_out_request.features_size2)) / (100 * 1000)
    return

match_counter = 0
cpu_selector = 0


def ssd_update_cpu_time(query_process_time, request):
    #print("++++++++++++++++ssd request update++++++++++++++++++")
    #print("query_process_time is: ")
    #print(query_process_time)
    #print("cpu_busy_end_time is: ")
    #print(cpu_busy_end_time)
    #print("request arrival time is: ")
    #print(request.arrival_time)
    for i in range(len(cpu_busy_end_time)):
        if cpu_busy_end_time[i] <= request.arrival_time:
            #print("found idle cpu")
            cpu_busy_end_time[i] = query_process_time + request.arrival_time
            #print("New cpu busy end time is: ")
            #print(cpu_busy_end_time[i])
            #print(cpu_busy_end_time)
            return query_process_time
    #print("every cpu is busy, wait and add wait time")
    #print(cpu_busy_end_time)
    wait_time = min(cpu_busy_end_time) - request.arrival_time
    query_process_time = wait_time + query_process_time
    cpu_busy_end_time[cpu_busy_end_time.index(min(cpu_busy_end_time))] = float(query_process_time + request.arrival_time)
    #print("updated time, wait and add wait time")
    #print("New cpu busy end time is: ")
    #print(cpu_busy_end_time)
    #print("new query_process_time is: ")
    #print(query_process_time)
    return query_process_time


def update_cpu_time(tmp_keyvalue, match_time, stime):
    #print("==============data center request update:================")
    for i in range(len(cpu_busy_end_time)):
        #print("cpu_busy_end_time is: ")
        #print(cpu_busy_end_time)
        #print("tmp_keyvalue.arrival_time is: ")
        #print(tmp_keyvalue.arrival_time)
        if cpu_busy_end_time[i] < tmp_keyvalue.arrival_time:
            #print("found idle cpu")
            #print("Match time is: ")
            #print(match_time)
            #print(stime)
            process_time = match_time - stime
            #print(process_time)
            #print(tmp_keyvalue.arrival_time)
            cpu_busy_end_time[i] = tmp_keyvalue.arrival_time + process_time
            #print(cpu_busy_end_time)
            #print("new cpu busy end time is: ")
            #print(cpu_busy_end_time[i])
            cpu_busy_flag = 0
            return process_time
    #print("every cpu is busy")
    #print(cpu_busy_end_time)
    wait_time = min(cpu_busy_end_time) - tmp_keyvalue.arrival_time
    process_time = wait_time + match_time - stime
    cpu_busy_end_time[cpu_busy_end_time.index(min(cpu_busy_end_time))] = float(process_time)
    #print("busy cpu update")
    #print(cpu_busy_end_time)
    return process_time


def main():
    global counter
    global match_counter
    global cpu_selector
    driver()
    #print("----------initial phase done--------------")
    while not incoming_queue.empty():
        stime = time.time()
        tmp_keyvalue = incoming_queue.get()
        bypass_flag = update_incoming_check_board(tmp_keyvalue)#datacenter cache
        cpu_selector = (cpu_selector + 1) % macro_define.CPU_NUMBER
        if bypass_flag == 1:
            match_counter += 1
            match_time = time.time()
            process_time = update_cpu_time(tmp_keyvalue, match_time, stime)
            user_response_time.append(process_time)
            tmp_keyvalue.finish_time = match_time - tmp_keyvalue.generate_time + tmp_keyvalue.arrival_time
            #process_time = (match_time - tmp_keyvalue.generate_time)/64
            finish_list.append(tmp_keyvalue)
        elif bypass_flag == 0:
            bypass_flag = update_query_check_board(tmp_keyvalue)#ISKEVA cache
            if bypass_flag == 1:
                match_counter += 1
                match_time = time.time()
                tmp_keyvalue.finish_time = match_time - tmp_keyvalue.generate_time + tmp_keyvalue.arrival_time
                #process_time = (match_time - stime)
                process_time = update_cpu_time(tmp_keyvalue, match_time, stime)
                user_response_time.append(process_time)
                finish_list.append(tmp_keyvalue)
            elif bypass_flag == 0:
                if tmp_keyvalue.operation == "WRITE":
                    continue
                    large_feature_req_pre_process(tmp_keyvalue)
                sending_out_queue.put(tmp_keyvalue)
                high_op_req_pre_process(tmp_keyvalue)
                sending_out_request = sending_out_queue.get()
                attrs = vars(tmp_keyvalue)
                #print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
                ssd_program_latency_update(sending_out_request)
                if sending_out_request.operation == "AND":
                    iskeva_and(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "OR":
                    iskeva_or(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "EXIST":
                    iskeva_exist(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "NOT":
                    iskeva_not(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "CNT":
                    iskeva_cnt(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "STORE_AND":
                    iskeva_store_and(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "STORE_OR":
                    iskeva_store_or(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "STORE_EXIST":
                    iskeva_store_exist(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "STORE_NOT":
                    iskeva_store_not(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "STORE_CNT":
                    iskeva_store_cnt(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                elif sending_out_request.operation == "WRITE":
                    iskeva_write(sending_out_request, stime, counter)
                    counter = (counter + 1) % macro_define.SSD_NUMBER
                else:
                    error_handler(sending_out_request)
        while not sending_out_queue.empty():
            tmp_keyvalue = sending_out_queue.get()
            sending_out_queue.put(tmp_keyvalue)
            sending_out_request = sending_out_queue.get()
            ssd_program_latency_update(sending_out_request)
            if sending_out_request.operation == "AND":
                iskeva_and(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            elif sending_out_request.operation == "OR":
                iskeva_or(sending_out_request, stime, counter)
            elif sending_out_request.operation == "EXIST":
                iskeva_exist(sending_out_request, stime, counter)
            elif sending_out_request.operation == "NOT":
                iskeva_not(sending_out_request, stime, counter)
            elif sending_out_request.operation == "CNT":
                iskeva_cnt(sending_out_request, stime, counter)
            elif sending_out_request.operation == "STORE_AND":
                iskeva_store_and(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            elif sending_out_request.operation == "STORE_OR":
                iskeva_store_or(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            elif sending_out_request.operation == "STORE_EXIST":
                iskeva_store_exist(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            elif sending_out_request.operation == "STORE_NOT":
                iskeva_store_not(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            elif sending_out_request.operation == "STORE_CNT":
                iskeva_store_cnt(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            elif sending_out_request.operation == "WRITE":
                iskeva_write(sending_out_request, stime, counter)
                counter = (counter + 1) % macro_define.SSD_NUMBER
            else:
                error_handler(sending_out_request)
    # #print("--- %s latency ---" % (time.time() - start_time + REQUEST_NUMBER * 0.016))
    # #print("--- %s throughput ---" % (10000 / (time.time() - start_time + REQUEST_NUMBER * 0.016)))
    #print(incoming_queue.qsize())
    #print(long_payload_queue.qsize())
    #print(normal_query_queue.qsize())
    #print("length of finish_list is :" + str(len(finish_list)))
    #print("-------analysis phase start----------")
    for i in finish_list:
        time_record.append(i.finish_time)
    #print(max(time_record))
    #print(macro_define.REQUEST_NUMBER / max(time_record))
    ##print(len(finish_list))
    for i in query_check_board:
        attrs = vars(i)
        ##print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
    ##print("-------------------------above is query check board-------------------------------------------------")
    for i in tmp_board:
        attrs = vars(i)
        ##print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items
    ##print(sending_out_queue)
    ##print("======================================================")
    ##print(sending_out_queue.empty())
    #for i in finish_list:
        #attrs = vars(i)
        ##print(', '.join("%s: %s" % item for item in attrs.items()))  # #print generated items

    for i in finish_list:
        time_record.append(i.finish_time)
    #print("MAX time is: ")
    #print(max(time_record))
    #print("average latency: ")
    #print(max(time_record) / macro_define.REQUEST_NUMBER)
    #print("solved request: ")
    #print(len(finish_list))
    #print(len(user_response_time))
    #print(user_response_time)
    # #print("compare results:", user_request_comparator(query_check_board[7], query_check_board[3]))
    print("Number of Request: " + str(macro_define.REQUEST_NUMBER))
    print("Number of SSD: " + str(macro_define.SSD_NUMBER))
    print("Number of Thread: " + str(macro_define.CPU_NUMBER))
    print("Average response time: ")
    print(sum(user_response_time)/macro_define.REQUEST_NUMBER)
    #print("Max response time is: ")
    #print(max(user_response_time))
    #print("match counter")
    #print(match_counter)
    #print("macro_define.REQUEST_NUMBER")
    #print(macro_define.REQUEST_NUMBER)
    print("Hit rate")
    print(match_counter/macro_define.REQUEST_NUMBER)
    print("------------------------------------------end---------------------------------------------")
    #print(cpu_busy_end_time)
    ##print(ssd_busy_end_time)
    return

#main()

i = 1000
numer_list = [1000, 3000, 5000, 8000, 10000, 20000]
while i <= 23000:
    i += 2000
    macro_define.REQUEST_NUMBER = i
    ssd_container = []
    ssd_busy_end_time = [0] * macro_define.SSD_NUMBER
    cpu_busy_end_time = [0] * macro_define.CPU_NUMBER
    pre_proc_flag = 0
    # used for check operation purpose
    query_check_board = []
    op_count_dict = {}  # count same operation frequency for pre-process
    flipped_op_count_dict = {}  # intermediate data structure for pre-process
    high_frq_dict = {}  # intermediate data structure for pre-process
    finish_list = []  # request used for analysis
    shortcut_finish_list = []
    time_record = []
    tmp_board = []
    user_response_time = []
    write_feature_list = []
    resultList = []
    incoming_query_list = []
    match_counter = 0
    cpu_selector = 0
    incoming_list = []
    main()

