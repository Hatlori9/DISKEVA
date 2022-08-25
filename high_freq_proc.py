import request_define
import macro_define




def pre_process(request):
    attrs = vars(request)
    op_count_dict[request.video_id] = request.operation, tuple(request.features)
    for key, value in op_count_dict.items():
        # value = tuple(value)
        if value not in flipped_op_count_dict:
            flipped_op_count_dict[value] = [key]
        else:
            flipped_op_count_dict[value].append(key)
    del (op_count_dict[request.video_id])
    #  for key, value in flipped_op_count_dict.items():
    #      for i in value:
    #         if value.count(i) != 1:
    #             for x in range((value.count(i) - 1)):
    #                value.remove(i)

    for key, value in flipped_op_count_dict.items():
        current_count = len(value)
        if current_count > int(macro_define.REQUEST_NUMBER * macro_define.HIGH_FREQ_QUERY_THRESHOLD):
            high_frq_dict[key] = value
            for i in stored_video:
                pre_proc_request = request_define.userrequest_type()
                pre_proc_request.length = i
                pre_proc_request.video_id = i
                pre_proc_request.features = list(key[1])
                pre_proc_request.operation = key[0]
                pre_proc_request.arrival_time = time.time()
                set2 = set(large_feature_pool)
                set3 = set(list(key[1]))
                if set3 & set2:
                    pre_proc_request.max_feature_size = 1
                else:
                    pre_proc_request.max_feature_size = 0
                pre_proc_request.priority = macro_define.MID_PRIORITY
                #bypass_flag = update_query_check_board(pre_proc_request)
                if int(pre_proc_request.length) > 5:
                    pre_proc_request.finish_time = macro_define.FREQ_OP_BYPASS_TIME
                    update_query_check_board(pre_proc_request)
                    if bypass_flag == 1:
                        pre_proc_request.finish_time = macro_define.FREQ_OP_BYPASS_TIME
                        finish_list.append(pre_proc_request)
                        tmp_board.append(pre_proc_request)
                    else:
                        sending_out_queue.put(pre_proc_request)

                else:
                    pre_proc_request.priority = macro_define.LOW_PRIORIRY
                    sending_out_queue.put(pre_proc_request)
                    update_query_check_board(pre_proc_request)
                    tmp_board.append(pre_proc_request)

                        # update_query_check_board(pre_proc_request)
    if current_count > int(macro_define.REQUEST_NUMBER * macro_define.HIGH_FREQ_QUERY_THRESHOLD):
        del flipped_op_count_dict[key]
    # print(high_frq_dict[key])
    return