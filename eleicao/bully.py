import argparse
import sys
import time
import socket
import random
from multiprocessing import Process, Manager
from inspect import currentframe, getframeinfo

def setup_argument_parser():
    parser = argparse.ArgumentParser()
    parser.add_argument('--num-of-processes', '-numproc', type=int, required=True, \
        help='pass the required number of processes')
    parser.add_argument('--detector-process', '-detectproc', type=int, required=True, \
        help='pass the process ID/priority which will detect initial coordinator failure')
    return parser.parse_args()

def connect_skt(skt, ip_address_and_port):
  while True:
        try:
            skt.connect(ip_address_and_port)
            break
        except ConnectionRefusedError:
            pass

def bind_and_listen_skt(skt, ip_address_and_port, max_num_connections):
    skt.bind(ip_address_and_port)
    skt.listen(max_num_connections)

def setup_skt(ip_address_and_port, mode):
    skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    if mode == 's':
        bind_and_listen_skt(skt, ip_address_and_port, len(shared_vars['ip_addresses_ports']) - 1)
    elif mode == 'c':
        connect_skt(skt, ip_address_and_port)
    else:
        print('ERROR: Correct mode not provided while calling setup_skt(), should be s/c\n', \
            end='', flush=True, file=debug_file)
        sys.exit(0)
    return skt

def run_proc(priority, shared_vars, lock, is_detector):
    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + '): priority ' + \
        str(priority) + ' has port ' + str(shared_vars['ip_addresses_ports'][priority][1]) + '\n', \
        end='', flush=True, file=debug_file)
    count = 0
    coordinator = True
    timeout = False
    s_skt = setup_skt(('', shared_vars['ip_addresses_ports'][priority][1]), 's')
    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + '): s_skt set by priority ' + \
        str(priority) + ' to ' + str(shared_vars['ip_addresses_ports'][priority][1]) + '\n', \
        end='', flush=True, file=debug_file)
    c_skts = {}
    lock.acquire()
    temp_shared_vars = shared_vars['lport_priority_links']
    for iterator in [iterator for iterator in range(1, len(shared_vars['ip_addresses_ports']) + 1) if iterator != priority]:
        c_skts[iterator] = setup_skt((shared_vars['ip_addresses_ports'][iterator][0], \
            shared_vars['ip_addresses_ports'][iterator][1]), 'c')
        # shared_vars['lport_priority_links'][iterator] = {}
        # print(str(shared_vars['lport_priority_links'][iterator]))
        # shared_vars['lport_priority_links'][iterator][c_skts[iterator].getsockname()[1]] = priority

        # shared_vars['lport_priority_links'][iterator][0].append(c_skts[iterator].getsockname()[1])
        # shared_vars['lport_priority_links'][iterator][1].append(priority)

        # the worst possible way to store this information, in a string, since manager.dict() can only handle mutating a non nested
        # object, so the above commented code (the first and second parts do the same thing, first being better than the second), 
        # though a better way to do it, does not work
        # apparently, mutating non nested data objects like list, dict, etc. also does not work
        # shared_vars['lport_priority_links'].append(str(iterator) + ':' + str(c_skts[iterator].getsockname()[1]) + ':' + \
        #     str(priority))

        # using a temp_shared_vars by assigning shared_vars['lport_priority_links'] to it, append to it 
        # and then reassign it to shared_vars['lport_priority_links']
        temp_shared_vars.append(str(iterator) + ':' + str(c_skts[iterator].getsockname()[1]) + ':' + str(priority))
    shared_vars['lport_priority_links'] = temp_shared_vars
    lock.release()
    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + '): c_skts set by priority ' + str(priority) + ' to ' + \
        str([shared_vars['ip_addresses_ports'][iterator][1] \
        for iterator in range(1, len(shared_vars['ip_addresses_ports']) + 1) if iterator != priority]) + '\n', \
        end='', flush=True, file=debug_file)
    new_skts = {}
    connection_accepts = []
    while len(shared_vars['lport_priority_links']) != len(shared_vars['ip_addresses_ports']) * \
        (len(shared_vars['ip_addresses_ports']) - 1):
        pass
    for _ in range(1, len(shared_vars['ip_addresses_ports'])):
        new_skt, addr = s_skt.accept()
        # new_skts[shared_vars['lport_priority_links'][priority][addr[1]]] = new_skt
        
        # new_skts[shared_vars['lport_priority_links'][priority][1][shared_vars['lport_priority_links'][priority][0].index(addr[1])]] = new_skt

        # see comments above
        for value in shared_vars['lport_priority_links']:
            if str(priority) + ':' + str(addr[1]) in value:
                new_skts[int(value.strip().split(':')[2])] = new_skt
                connection_accepts.append(int(value.strip().split(':')[2]))
                break
    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + '): connections accepted by priority ' + \
        str(priority) + ' from priorities ' + str(connection_accepts) + '\n', \
        end='', flush=True, file=debug_file)
    while True:
        if shared_vars['coordinator_id'] == priority and coordinator:
            print('coordinator:' + str(priority) + '\n', end='', flush=True, file=output_file)
            coordinator = False

        # work done by each process
        print('work:' + str(priority) + '\n', end='', flush=True, file=output_file)

        time.sleep(5)
        count += 1
        if is_detector:
            while not timeout:
                while True:
                    try:
                        c_skts[shared_vars['coordinator_id']].sendall(('heartbeat:' + str(priority)).encode('utf-8'))
                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                            '): sent heartbeat message to priority ' + str(shared_vars['coordinator_id']) + \
                            ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                        new_skts[shared_vars['coordinator_id']].settimeout(10)
                        break
                    except ConnectionResetError:
                        pass
                try:
                    response = new_skts[shared_vars['coordinator_id']].recv(256).decode('ascii').strip().split(':')
                    new_skts[shared_vars['coordinator_id']].settimeout(None)
                    if response[0] != 'ack' or int(response[1]) != shared_vars['coordinator_id']:
                        print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                            '): got an invalid response or got a valid response but not from the coordinator\n', \
                            end='', flush=True, file=debug_file)
                        sys.exit(0)
                    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                        '): received ack message from priority ' + str(shared_vars['coordinator_id']) + \
                        ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                except socket.timeout:
                    new_skts[shared_vars['coordinator_id']].settimeout(None)
                    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                        '): did not receive ack message within time from priority ' + str(shared_vars['coordinator_id']) + \
                        ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                    timeout = True
                    if priority == len(shared_vars['ip_addresses_ports']) - 1:
                        lock.acquire()
                        shared_vars['coordinator_id'] = priority
                        lock.release()
                        for key in sorted(c_skts.keys())[:-1]:
                            # if key != len(shared_vars['ip_addresses_ports']):
                            while True:
                                try:
                                    c_skts[key].sendall(('coordinator:' + str(priority)).encode('utf-8'))
                                    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                        '): sent coordinator message to priority ' + str(key) + \
                                        ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                    break
                                except ConnectionResetError:
                                    pass
                    else:
                        lock.acquire()
                        shared_vars['coordinator_id'] = -1
                        lock.release()

                        # send an election msg to all priorities above and wait for their responses
                        # wait for an alive msg from at least one priority above it within some time
                        # wait for a coordinator msg within some time

                        num_alive = 0
                        for key in sorted(c_skts.keys())[priority - 1:-1]:
                            while True:
                                try:
                                    c_skts[key].sendall(('election:' + str(priority)).encode('utf-8'))
                                    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                        '): sent election message to priority ' + str(key) + \
                                        ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                    new_skts[key].settimeout(10)
                                    break
                                except ConnectionResetError:
                                    pass
                            try:
                                response = new_skts[key].recv(256).decode('ascii').strip().split(':')
                                new_skts[key].settimeout(None)
                                if response[0] != 'alive' or int(response[1]) != key:
                                    print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                        '): got an invalid response or got a valid response but not from one of the required priorities\n', \
                                        end='', flush=True, file=debug_file)
                                    sys.exit(0)
                                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                    '): received alive message from priority ' + str(key) + \
                                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                num_alive += 1
                            except socket.timeout:
                                new_skts[key].settimeout(None)
                                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                    '): did not receive alive message within time from priority ' + str(key) + \
                                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                        if num_alive > 0:
                            for key in sorted(c_skts.keys())[priority - 1:-1]:
                                try:
                                    new_skts[key].settimeout(10)
                                    response = new_skts[key].recv(256).decode('ascii').strip().split(':')
                                    new_skts[key].settimeout(None)
                                    if response[0] != 'coordinator' or int(response[1]) != key:
                                        print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                            '): got an invalid response or got a valid response but not from one of the required priorities\n', \
                                            end='', flush=True, file=debug_file)
                                        sys.exit(0)
                                    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                        '): received coordinator message from priority ' + str(key) + \
                                        ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                except socket.timeout:
                                    new_skts[key].settimeout(None)
                                    print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                        '): did not receive coordinator message within time from priority ' + str(key) + \
                                        ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                        else:
                            lock.acquire()
                            shared_vars['coordinator_id'] = priority
                            lock.release()
                            for key in sorted(c_skts.keys())[:priority]:
                                while True:
                                    try:
                                        c_skts[key].sendall(('coordinator:' + str(priority)).encode('utf-8'))
                                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                            '): sent coordinator message to priority ' + str(key) + \
                                            ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                        break
                                    except ConnectionResetError:
                                        pass
                        break
            if count == 15:

                # wait for a coordinator msg from the initial coordinator

                while shared_vars['coordinator_id'] != len(shared_vars['ip_addresses_ports']):
                    pass
                response = new_skts[shared_vars['coordinator_id']].recv(256).decode('ascii').strip().split(':')
                if response[0] != 'coordinator' or int(response[1]) != shared_vars['coordinator_id']:
                    print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                        '): got an invalid response or got a valid response but not from the coordinator\n', \
                        end='', flush=True, file=debug_file)
                    sys.exit(0)
                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                    '): received coordinator message from priority ' + str(shared_vars['coordinator_id']) + \
                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
            if count == 20:
                break
        elif priority != shared_vars['coordinator_id']:
            if count == 6:

                # wait for an election msg from the detector and send response (only processes with higher priorities need to wait)
                # then send an alive msg to the detector (only processes with higher priorities have to send)
                # process with current highest priority will then send a coordinator msg to all the processes
                # else wait for a coordinator msg in case the detector is the new coordinator (all processes need to wait)

                response = ''
                if priority > shared_vars['detector_id']:
                    try:
                        new_skts[shared_vars['detector_id']].settimeout(15)
                        response = new_skts[shared_vars['detector_id']].recv(256).decode('ascii').strip().split(':')
                        new_skts[shared_vars['detector_id']].settimeout(None)
                        if response[0] != 'election' and response[0] != 'coordinator' or \
                            int(response[1]) != shared_vars['detector_id']:
                            print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): got an invalid response or got a valid response but not from the detector\n', \
                                end='', flush=True, file=debug_file)
                            sys.exit(0)
                        if response[0] == 'election':
                            print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): received election message from priority ' + str(shared_vars['detector_id']) + \
                                ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                        elif response[0] == 'coordinator':
                            # should also be able to use shared_vars['coordinator_id'] here
                            print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): received coordinator message from priority ' + str(shared_vars['detector_id']) + \
                                ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                    except socket.timeout:
                        new_skts[shared_vars['detector_id']].settimeout(None)
                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                            '): did not receive election or coordinator message within time from priority ' + \
                            str(shared_vars['detector_id']) + ' by priority ' + str(priority) + '\n', \
                            end='', flush=True, file=debug_file)
                    # TODO here the process with current highest priority sends the coordinator msg to all the processes with lesser
                    # priority than itself after sending alive msg to the detector, though processes with higher priority than the
                    # detector just send the alive msg to the detector, after this they should again repeat the entire algo until
                    # they receive a coordinator msg from the highest priority process due to the initial election started by the
                    # detector
                    if response[0] == 'election':
                        while True:
                            try:
                                c_skts[shared_vars['detector_id']].sendall(('alive:' + str(priority)).encode('utf-8'))
                                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                    '): sent alive message to priority ' + str(shared_vars['detector_id']) + \
                                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                break
                            except ConnectionResetError:
                                pass
                        if priority == len(shared_vars['ip_addresses_ports']) - 1:
                            lock.acquire()
                            shared_vars['coordinator_id'] = priority
                            lock.release()
                            for key in sorted(c_skts.keys())[:-1]:
                                while True:
                                    try:
                                        c_skts[key].sendall(('coordinator:' + str(priority)).encode('utf-8'))
                                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                            '): sent coordinator message to priority ' + str(key) + \
                                            ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                                        break
                                    except ConnectionResetError:
                                        pass
                else:
                    try:
                        new_skts[shared_vars['detector_id']].settimeout(15)
                        response = new_skts[shared_vars['detector_id']].recv(256).decode('ascii').strip().split(':')
                        new_skts[shared_vars['detector_id']].settimeout(None)
                        if response[0] != 'coordinator' or int(response[1]) != shared_vars['detector_id']:
                            print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): got an invalid response or got a valid response but not from the detector\n', \
                                end='', flush=True, file=debug_file)
                            sys.exit(0)
                        elif response[0] == 'coordinator':
                            # should also be able to use shared_vars['coordinator_id'] here
                            print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): received coordinator message from priority ' + str(shared_vars['detector_id']) + \
                                ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                    except socket.timeout:
                        new_skts[shared_vars['detector_id']].settimeout(None)
                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                            '): did not receive coordinator message within time from priority ' + str(shared_vars['detector_id']) + \
                            ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                    try:
                        new_skts[shared_vars['coordinator_id']].settimeout(15)
                        response = new_skts[shared_vars['coordinator_id']].recv(256).decode('ascii').strip().split(':')
                        new_skts[shared_vars['coordinator_id']].settimeout(None)
                        if response[0] != 'coordinator' or int(response[1]) != shared_vars['coordinator_id']:
                            print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): got an invalid response or got a valid response but not from the coordinator\n', \
                                end='', flush=True, file=debug_file)
                            sys.exit(0)
                        elif response[0] == 'coordinator':
                            print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): received coordinator message from priority ' + str(shared_vars['coordinator_id']) + \
                                ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                    except socket.timeout:
                        new_skts[shared_vars['detector_id']].settimeout(None)
                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                            '): did not receive coordinator message within time from priority ' + str(shared_vars['coordinator_id']) + \
                            ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
            if count == 15:

                # wait for a coordinator msg from the initial coordinator

                while shared_vars['coordinator_id'] != len(shared_vars['ip_addresses_ports']):
                    pass
                response = new_skts[shared_vars['coordinator_id']].recv(256).decode('ascii').strip().split(':')
                if response[0] != 'coordinator' or int(response[1]) != shared_vars['coordinator_id']:
                    print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                        '): got an invalid response or got a valid response but not from coordinator\n', \
                        end='', flush=True, file=debug_file)
                    sys.exit(0)
                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                    '): received coordinator message from priority ' + str(shared_vars['coordinator_id']) + \
                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
            if count == 20:
                break
        if shared_vars['coordinator_id'] == priority and priority == len(shared_vars['ip_addresses_ports']) - 1:
            if count == 15:
                while shared_vars['coordinator_id'] != len(shared_vars['ip_addresses_ports']):
                    pass
                response = new_skts[shared_vars['coordinator_id']].recv(256).decode('ascii').strip().split(':')
                if response[0] != 'coordinator' or int(response[1]) != shared_vars['coordinator_id']:
                    print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                        '): got an invalid response or got a valid response but not from coordinator\n', \
                        end='', flush=True, file=debug_file)
                    sys.exit(0)
                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                    '): received coordinator message from priority ' + str(shared_vars['coordinator_id']) + \
                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
        if shared_vars['coordinator_id'] == priority and priority == len(shared_vars['ip_addresses_ports']):
            if count <= 5:
                request = new_skts[shared_vars['detector_id']].recv(256).decode('ascii').strip().split(':')
                if request[0] != 'heartbeat' or int(request[1]) != shared_vars['detector_id']:
                    print('ERROR lineno(' + str(getframeinfo(currentframe()).lineno) + \
                        '): got an invalid request or got a valid request but not from detector\n', \
                        end='', flush=True, file=debug_file)
                    sys.exit(0)
                print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                    '): received heartbeat message from priority ' + str(shared_vars['detector_id']) + \
                    ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                while True:
                    try:
                        c_skts[shared_vars['detector_id']].sendall(('ack:' + str(priority)).encode('utf-8'))
                        print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                            '): sent ack message to priority ' + str(shared_vars['detector_id']) + \
                            ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                        break
                    except ConnectionResetError:
                        pass
            if count == 6:
                print('failed:' + str(priority) + '\n', end='', flush=True, file=output_file)
                time.sleep(120)
                print('recovered:' + str(priority) + '\n', end='', flush=True, file=output_file)
                lock.acquire()
                shared_vars['coordinator_id'] = priority
                lock.release()
                coordinator = True
                for key in sorted(c_skts.keys()):
                    while True:
                        try:
                            c_skts[key].sendall(('coordinator:' + str(priority)).encode('utf-8'))
                            print('DEBUG lineno(' + str(getframeinfo(currentframe()).lineno) + \
                                '): sent coordinator message to priority ' + str(key) + \
                                ' by priority ' + str(priority) + '\n', end='', flush=True, file=debug_file)
                            break
                        except ConnectionResetError:
                            pass
            if count == 20:
                break

if __name__ == '__main__':

    debug_file = open('debug.txt', 'w')
    output_file = open('output.txt', 'w')

    # setting up input of command line arguments
    args = setup_argument_parser()
    num_of_processes = args.num_of_processes
    detector_process = args.detector_process
    if num_of_processes <= 2:
        print('ERROR: Number of processes must be greater than 2\n', \
            end='', flush=True, file=debug_file)
        sys.exit(0)
    if detector_process == num_of_processes:
        print('ERROR: The detector process cannot be the initial coordinator\n', \
            end='', flush=True, file=debug_file)
        sys.exit(0)
    elif detector_process <= 0:
        print('ERROR: The detector process cannot have ID/priority less than or equal to 0\n', \
            end='', flush=True, file=debug_file)
        sys.exit(0)
    elif detector_process > num_of_processes:
        print('ERROR: The detector process cannot have ID/priority greater than the number of processes\n', \
            end='', flush=True, file=debug_file)
        sys.exit(0)

    # initializing ip address and port for all processes (localhost is used for all processes)
    base_port = random.randrange(9000, 20000, num_of_processes + 1)
    ip_addresses_ports = {}
    for iterator in range(1, num_of_processes + 1):
        ip_addresses_ports[iterator] = ('127.0.0.1', base_port + iterator)

    manager = Manager()
    lock = manager.Lock()
    shared_vars = manager.dict()
    jobs = []

    # setting variables to be shared among all processes
    shared_vars['ip_addresses_ports'] = ip_addresses_ports
    shared_vars['coordinator_id'] = num_of_processes
    shared_vars['detector_id'] = detector_process
    shared_vars['lport_priority_links'] = []

    # process id is the same as process priority
    for process_id in range(num_of_processes, 0, -1):
        if process_id == detector_process:
            process = Process(target=run_proc, args=(process_id, shared_vars, lock, True))
        else:
            process = Process(target=run_proc, args=(process_id, shared_vars, lock, False))
        jobs.append(process)
        process.start()
    for process in jobs:
        process.join()

    debug_file.close()
    output_file.close()