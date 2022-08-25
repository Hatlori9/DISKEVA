import main


def iskeva_and(request, stime, counter):
    # ------------------------------------

    # occupied_ssd_id = random.randint(1, 6)  # random select SSD

    # ------------------------------------
    if len(main.ssd_container) == 1:
        occupied_ssd_id = 0
    else:
        occupied_ssd_id = counter  # Round_robin
    # ------------------------------------
    finish_time = 0
    main.ssd_container[occupied_ssd_id - 1].state = "BUSY"
    iskeva_and_queue.put(request)  # fetch from data center qu
    and_etime = time.time()
    if ssd_container[occupied_ssd_id - 1].state == "BUSY":
        # print("request arrival, ssd is busy")
        etime_before_ssd_proc = time.time()
        server_etime_before_ssd_proc = etime_before_ssd_proc - stime
        # print("request.arrival_time")
        # print(request.arrival_time)
        # print("server_etime_before_ssd_proc")
        # print(server_etime_before_ssd_proc)
        # print("ssd_busy_end_time[occupied_ssd_id - 1]")
        # print(ssd_busy_end_time[occupied_ssd_id - 1])
        # if (request.arrival_time + server_etime_before_ssd_proc) < ssd_busy_end_time[occupied_ssd_id - 1]:
        if (0 + server_etime_before_ssd_proc) < ssd_busy_end_time[occupied_ssd_id - 1]:
            if request.priority == macro_define.HIGH_PRIORITY:
                # finish_time = request.arrival_time + (and_etime - stime) + ssd_process_time + ssd_busy_end_time[counter - 1]
                finish_time = 0 + 0 + ssd_process_time + ssd_busy_end_time[counter - 1]
                request.finish_time = finish_time
                update_ssd_state(finish_time, occupied_ssd_id, stime, request)
            else:
                finish_time = stime - request.arrival_time + ssd_process_time + ssd_busy_end_time[counter - 1]
                request.finish_time = finish_time
                update_ssd_state(finish_time, occupied_ssd_id, request.arrival_time, request.arrival_time)
        else:
            if request.priority == macro_define.HIGH_PRIORITY:
                # finish_time = request.arrival_time + (and_etime - stime) + ssd_process_time
                finish_time = 0 + 0 + ssd_process_time
                request.finish_time = finish_time
                update_ssd_state(finish_time, occupied_ssd_id, stime, request)
            else:
                finish_time = stime - request.arrival_time + ssd_process_time
                request.finish_time = finish_time
                update_ssd_state(finish_time, occupied_ssd_id, request.arrival_time, request.arrival_time)
        finish_list.append(request)
        # finish time = query commited by
        # clints - SSD commited the request + constant ssd processing time
        # print(finish_time)
        # print("finish_time shows above")
        return
    return


def iskeva_or(request, stime):
    ssd_process_time = 0.016
    occupied_ssd_id = random.randint(1, 6)
    iskeva_or_queue.put(request)
    if ssd_container[occupied_ssd_id - 1].state == "BUSY":
        finish_time = process_time - request.arrival_time + ssd_process_time
        return finish_time
    else:
        ssd_container[occupied_ssd_id - 1].state == "BUSY"
        iskeva_or_queue.get(request)
        process_time = time.time()
        finish_time = process_time - request.arrival_time + ssd_process_time * 2
        return finish_time
    return finish_time


def iskeva_exist(request, stime):
    ssd_process_time = 0.016
    occupied_ssd_id = random.randint(1, 6)
    iskeva_exist_queue.put(request)
    if ssd_container[occupied_ssd_id - 1].state == "BUSY":
        finish_time = process_time - request.arrival_time + ssd_process_time
        return finish_time
    else:
        ssd_container[occupied_ssd_id - 1].state == "BUSY"
        iskeva_exist_queue.get(request)
        process_time = time.time()
        finish_time = process_time - request.arrival_time + ssd_process_time * 2
        return finish_time
    return finish_time


def iskeva_not(request, stime):
    ssd_process_time = 0.016
    occupied_ssd_id = random.randint(1, 6)
    iskeva_not_queue.put(request)
    if ssd_container[occupied_ssd_id - 1].state == "BUSY":
        finish_time = process_time - request.arrival_time + ssd_process_time
        return finish_time
    else:
        ssd_container[occupied_ssd_id - 1].state == "BUSY"
        iskeva_not_queue.get(request)
        process_time = time.time()
        finish_time = process_time - request.arrival_time + ssd_process_time * 2
        return finish_time
    return finish_time


def iskeva_cnt(request, stime):
    ssd_process_time = 0.016
    occupied_ssd_id = random.randint(1, 6)
    iskeva_cnt_queue.put(request)
    if ssd_container[occupied_ssd_id - 1].state == "BUSY":
        finish_time = process_time - request.arrival_time + ssd_process_time
        return finish_time
    else:
        ssd_container[occupied_ssd_id - 1].state == "BUSY"
        iskeva_cnt_queue.get(request)
        process_time = time.time()
        finish_time = process_time - request.arrival_time + ssd_process_time * 2
        return finish_time
    return


def error_handler(request):
    print("fatal error, invalid op")
    return false