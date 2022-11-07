from dummy_threading import local
import socket
import time
import threading
import json
from xml.etree.ElementTree import TreeBuilder
import utils
import logging
from logging.handlers import RotatingFileHandler
import datetime
import mp1_client
import mp1_server
import os

HOST = socket.gethostname()
IP = socket.gethostbyname(HOST)
MAIN_PORT = 23333
FILE_PORT = 23334
FILE_PORT_2 = 23335
FILE_DIRECTORY = 'files'
BUFFER_SIZE = 4096

# commands
PUT = "PUT"
GET = "GET"
DELETE = "DEL"
GET_VERSIONS = "GET-V"
TRANSFER = "TRANSFER"
HOST_FAIL = "HOST_FAIL"
MODIFY_ADD = "MODIFY_ADD"
FILE_COMMANDS = (PUT, GET, DELETE, GET_VERSIONS, TRANSFER, MODIFY_ADD)



# define file logging info
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    filename=  'host.log',
                    filemode='w')
# define a handler that displays ERROR messages to the terminal
console = logging.StreamHandler()
console.setLevel(logging.ERROR)
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)
rotating_file_handler = RotatingFileHandler('host.log', maxBytes=102400000, backupCount=1)
logging.getLogger('').addHandler(rotating_file_handler)
recv_logger = logging.getLogger('receiver')
monitor_logger = logging.getLogger('monitor')
join_logger = logging.getLogger('join')
send_logger = logging.getLogger('send')
file_logger = logging.getLogger('file')


class Server:
    def __init__(self):
        timestamp = str(int(time.time()))
        # membership list, key: host, value: (timestamp, status)
        self.MembershipList = {
            HOST: (timestamp, utils.Status.LEAVE)}
        self.FILES = {}
        for host in utils.get_all_hosts():
            self.FILES[host] = []
        self.time_lock = threading.Lock()
        self.ml_lock = threading.Lock()
        self.file_lock = threading.Lock()
        # record the time current process receives last ack from its neighbors
        self.last_update = {}

    # get name of latest version, as well as the version of the new file to be stored
    def get_latest_version(self, filename):
        allfiles = os.listdir(FILE_DIRECTORY)
        vc = 0
        max_v = -1
        max_version_filename = ""
        print("runit")
        for f in allfiles:
            val = f.split('_')
            fn = val[0]
            v = val[1]
            if (fn == filename):
                vc += 1
            if (int(v) > max_v):
                max_v = int(v)
                max_version_filename = f
        print("once")
        return max_version_filename, vc

    # delete all versions of a file
    def delete_all_files(self, filename):
        allfiles = os.listdir(FILE_DIRECTORY)
        for f in allfiles:
            fn, v = f.split('_')
            if (fn == filename):
                os.remove(os.path.join(FILE_DIRECTORY, f))

    # create superfile which is all versions concatenated (to send back to user)
    def create_version_superfile(self, filename, version):
        check, max_version = self.get_latest_version(filename)
        nf = open(filename + "_superfile", "w")
        allfiles = os.listdir(FILE_DIRECTORY)
        
        for f in allfiles:
            val = f.split('_')
            fn = val[0]
            v = val[1]
        
            if (fn == filename) and int(v) > max_version - version - 1:
                print("just wrote version " + v)
                curr_version_filepath = os.path.join(FILE_DIRECTORY, f)
                cvf = open(curr_version_filepath, "r")
                nf.write(cvf.read())
                nf.write('\n; just wrote version ' + v)
                cvf.close()
        nf.close()
        return filename

    # New process joining: must contact introducer
    def join(self):
        '''
        Contacts the introducer that the process will join the group and uptate its status.

        return: None
        '''
        print("start joining")
        timestamp = str(int(time.time()))
        join_logger.info("Encounter join before:")
        join_logger.info(self.MembershipList)
        self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
        join_logger.info("Encounter after before:")
        join_logger.info(self.MembershipList)
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if HOST != utils.INTRODUCER_HOST:
            join_msg = [utils.Type.JOIN, HOST, self.MembershipList[HOST]]
            s.sendto(json.dumps(join_msg).encode(), (utils.INTRODUCER_HOST, MAIN_PORT))
        else:
            print("This is introducer host!")

    # Handle file commands
    def handle_file(self):
        # input command, filename, local filepath (if required)
        command = input("Please enter desired file command. PUT for upload, GET for download, DEL for delete.")
        command = command.upper()
        if (command not in FILE_COMMANDS):
            print("Invalid file command!")
            return 1
        filename = input("Please enter the name of the file you'd like to perform the command on.")
        filepath = ""
        if (command == PUT or command == GET or command == GET_VERSIONS):
            filepath = input("Please enter local filepath.")
            if (command == PUT and not os.path.exists(filepath)):
                print("File at \"" + filepath + "\" does not exist!")
                return 2

        # call relevant function
        if (command == PUT):
            self.upload(filename, filepath)
        elif (command == GET or command == GET_VERSIONS):
            self.download(command, filename, filepath)
        elif (command == DELETE):
            self.delete(filename)

    # send file
    def send_file(self, command, filename, filepath, host):
        print("goes in")
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:

            try:
                # Step 1: Send file metadata (command, filename, filesize)
                filesize = os.path.getsize(filepath)
                print("sender" + str(filesize))
                s.sendto(json.dumps({"COMMAND": PUT, "FILENAME": filename, "FILESIZE": filesize}).encode('utf-8'), (host, FILE_PORT))
                print("SENDS")
                # Step 2: Send file data (in chunks of 4096 bytes)
                with open(filepath, "rb") as f:
                    while True:
                        bytes_read = f.read(BUFFER_SIZE)
                        if not bytes_read:
                            break
                        s.sendto(bytes_read, (host, FILE_PORT))
            except Exception as e:
                print(e)

    # upload file
    def upload(self, filename, filepath):
        print('Attempting to upload \"' + filename + "\" at location \"" + filepath + "\"!")
        

        # we will send file to ourselves AND next three in the ring for redundancy
        print("BEFORE")
        before_time = time.time()
        print(socket.gethostbyname(HOST))
        for host in utils.get_neighbors(HOST):
            self.send_file(PUT, filename, filepath, host)
        print("here")
        self.send_file(PUT, filename, filepath, HOST)
        after_time = time.time()
        print("total upload time:", after_time - before_time)
        print("AFTER")

       
        # send out message to every server about where file was uploaded
        # so they can update their global maps

    # download file
    def download(self, command, filename, filepath):
        print('Attempting to download \"' + filename + "\" to location \"" + filepath + "\"!")
        check = True
        before_time = time.time()
        if command == GET_VERSIONS:
            num_version = input("Please enter how many version you want.")
            local_filepath = self.create_version_superfile(filename, int(num_version))
            return
        for check_host in self.FILES:
            if filename in self.FILES[check_host] and self.MembershipList[check_host][1] == utils.Status.RUNNING:
                check = False
                if not check_host == HOST:
                    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                        s.bind((HOST, FILE_PORT_2))
                        try:
                            # Step 1: Send file metadata (command, filename, filesize [redundant])

                            s.sendto(json.dumps({"COMMAND": command, "FILENAME": filename, "FILESIZE": 0, "HOST": HOST}).encode('utf-8'), (check_host, FILE_PORT))

                            # Step 2: Get file data (in chunks of 4096 bytes)
                            data, _ = s.recvfrom(BUFFER_SIZE)
                            request = data.decode('utf-8')
                            request_list = json.loads(request)
                            filesize = request_list['FILESIZE']
                            print("receiver" + str(filesize))
                            bytes_written = 0
                            with open(filepath, "wb") as f:
                                while bytes_written < filesize:
                                    bytes_read, _ = s.recvfrom(BUFFER_SIZE)
                                    f.write(bytes_read)
                                    bytes_written += len(bytes_read)
                        except Exception as e:
                            print(e)
        
        after_time = time.time()
        if check:
            print("Sorry but the given file is not in the file system")

        print("total download time:", after_time - before_time)

    # delete file
    def delete(self, filename):
        for host in utils.get_all_hosts():
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.sendto(json.dumps({"COMMAND": DELETE, "FILENAME": filename, "FILESIZE": 0}).encode('utf-8'), (host, FILE_PORT))

    # get-versions command
    def get_versions(self, filename, filepath):
        print("getting all versions of", filename, "and saving to", filepath)

    # general program to handle file requests
    # this runs on its own thread and uses its own own port
    def file_program(self):
        print("file program started")
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((HOST, FILE_PORT))
        print(HOST)
        file_logger.info('file program started')
        while True:
            print("running!")
            try:
                print("try please")
                data, addr = s.recvfrom(BUFFER_SIZE)
                print("received again")
                file_logger.info("FILE connection from: " + str(addr) + " with data: " + data.decode())
                if data:
                    # Receive Header
                    request = data.decode('utf-8')
                    request_list = json.loads(request)
                    
                    command = request_list['COMMAND']
                                
                    self.file_lock.acquire()

                    # Handle the actual file command
                    if command == PUT:
                        filesize = request_list['FILESIZE']
                        filename = request_list['FILENAME']
                        print('Saving file \"' + filename + "\".")
                        latest_version_filepath, latest_version = self.get_latest_version(filename)
                        local_filepath = os.path.join(FILE_DIRECTORY, filename + '_' + str(latest_version))
                        bytes_written = 0
                    
                        with open(local_filepath, "wb") as f:
                       
                            while bytes_written < filesize:
                
                                bytes_read, _ = s.recvfrom(BUFFER_SIZE)
                     
                                f.write(bytes_read)
                         
                                bytes_written += len(bytes_read)

                        for host in utils.get_all_hosts():
                            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as m:
                                time.sleep(0.1)
                                m.sendto(json.dumps({"COMMAND": MODIFY_ADD, "FILENAME": filename, "HOST": HOST}).encode('utf-8'), (host, FILE_PORT))

                    elif command == GET:
                        filesize = request_list['FILESIZE']
                        filename = request_list['FILENAME']
                        print('Sending file \"' + filename + "\".")
                        
                        local_filepath = ""
                        print("befre")
                        local_filepath, _ = self.get_latest_version(filename)
                        local_filepath = os.path.join(FILE_DIRECTORY, local_filepath)
                        print("after " + local_filepath)
                        

                        host = request_list["HOST"]
                        # Send size first
                        filesize = os.path.getsize(local_filepath)
                        print("HERE")
                        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as m:
                            m.sendto(json.dumps({"FILESIZE": filesize}).encode('utf-8'), (host, FILE_PORT_2))
                            with open(local_filepath, "rb") as f:
                                while True:
                                    bytes_read = f.read(BUFFER_SIZE)
                                    if not bytes_read:
                                        break
                                    m.sendto(bytes_read, (host, FILE_PORT_2))

                        time.sleep(3)
                    
                    elif command == DELETE:
                        filename = request_list['FILENAME']
                        print('Deleting file \"' + filename + "\".")
                        
                 
                        self.delete_all_files(filename)
                        for check_host in self.FILES:
                            if filename in self.FILES[check_host]:
                                self.FILES[check_host].remove(filename)
                                
                    elif command == HOST_FAIL:
                        host = request_list["HOST"]
                        self.FILES[host] = []

                    elif command == MODIFY_ADD:
                        filename = request_list['FILENAME']
                        host = request_list["HOST"]
                        if filename not in self.FILES[host]:
                            self.FILES[host].append(filename)
                        print("ADDED FROM " + host)

                    
                    elif command == TRANSFER:
                        print('saving file without version updates')

                    elif (command == GET_VERSIONS):
                        print('sending superfile with all versions concatenated')

                    else:
                        print("Unknown file command! Try again!")

                    self.file_lock.release()


            except Exception as e:
                print(e)


    def send_ping(self, host):
        '''
        Send PING to current process's neighbor using UDP. If the host is leaved/failed, then do nothing.

        return: None
        '''
        
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        while True:
            time.sleep(0.3)
            if self.MembershipList[HOST][1] == utils.Status.LEAVE or host not in self.MembershipList or self.MembershipList[host][1] == utils.Status.LEAVE:
                continue
            try:
                self.ml_lock.acquire()
                timestamp = str(int(time.time()))
                send_logger.info("Encounter send before:")
                send_logger.info(self.MembershipList)
                self.MembershipList[HOST] = (timestamp, utils.Status.RUNNING)
                send_logger.info("Encounter send after:")
                send_logger.info(self.MembershipList)
                
                ping_msg = [utils.Type.PING, HOST, self.MembershipList]
                s.sendto(json.dumps(ping_msg).encode(), (host, MAIN_PORT))
                if host in self.MembershipList and host not in self.last_update:
                    self.time_lock.acquire()
                    self.last_update[host] = time.time()
                    self.time_lock.release()
                self.ml_lock.release()
            except Exception as e:
                print(e)

    def receiver_program(self):
        '''
        Handles receives in different situations: PING, PONG and JOIN
        When reveived PING: update membership list and send PONG back to the sender_host
        When received PONG: delete the sender_host from last_update table and update membership list
        When received JOIN: update the membershi list and notify other hosts if you are the introducer host
        
        return: None
        '''
        print("receiver started")
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((HOST, MAIN_PORT))
        recv_logger.info('receiver program started')
        while True:
            try:
                if self.MembershipList[HOST][1] == utils.Status.LEAVE:
                    recv_logger.info("skip receiver program since " + HOST + " is leaved")
                    continue
                data, addr = s.recvfrom(4096)
                recv_logger.info("connection from: " + str(addr) + " with data: " + data.decode())
                if data:
                    request = data.decode()
                    request_list = json.loads(request)
                    sender_host = request_list[1]
                    request_type = request_list[0]
                    
                    request_membership = request_list[2]
            
                    self.ml_lock.acquire()
                    if request_type == utils.Type.JOIN:
                        recv_logger.info("Encounter join before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        
                        self.MembershipList[sender_host] = (str(int(time.time())), utils.Status.NEW)
                        recv_logger.info("Encounter join after:")
                        recv_logger.info(json.dumps(self.MembershipList))


                        
                        if HOST == utils.INTRODUCER_HOST:
                            recv_logger.info("introducer recv connection from new joiner: " + str(addr))
                            
                            join_msg = [utils.Type.JOIN, sender_host, self.MembershipList[sender_host]]
                            hosts = utils.get_all_hosts()
                            ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                            for hostname in hosts:
                                if hostname != HOST and hostname != sender_host:
                                    ss.sendto(json.dumps(join_msg).encode(), (hostname, MAIN_PORT))

                    elif request_type == utils.Type.PING:
                        recv_logger.info("Encounter PING before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        for host, value in request_membership.items():
                            timestamp, status = value[0], value[1]
                            if status == utils.Status.LEAVE:
                                self.MembershipList[host] = value

                            if host not in self.MembershipList:
                                self.MembershipList[host] = value
                                continue
                        
                            if int(timestamp) > int(self.MembershipList[host][0]):
                                self.MembershipList[host] = (timestamp, status)
                        recv_logger.info("Encounter PING after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        pong = [utils.Type.PONG, HOST, self.MembershipList[HOST]]
                        
                        s.sendto(json.dumps(pong).encode(), (sender_host, MAIN_PORT))

                    elif request_type == utils.Type.PONG:
                        recv_logger.info("Encounter PONG before:")
                        recv_logger.info(json.dumps(self.MembershipList))
                        self.MembershipList[sender_host] = request_membership
                        if sender_host in self.last_update:
                            self.time_lock.acquire()
                            self.last_update.pop(sender_host, None)
                            self.time_lock.release()
                        recv_logger.info("Encounter PONG after:")
                        recv_logger.info(json.dumps(self.MembershipList))
                    else:
                        recv_logger.error("Unknown message type")
                    self.ml_lock.release()
            except Exception as e:
                print(e)

    def monitor_program(self):
        '''
        Monitor daemon that checks if any neighbor process has timeout

        return: None
        '''
        print("monitor started")
        while True:
            try:
                self.time_lock.acquire()
                
                keys = list(self.last_update.keys())
                for hostname in keys:
                    if time.time() - self.last_update[hostname] > 2:
                        value = self.MembershipList.get(hostname, "*")
                        if value != "*" and value[1] != utils.Status.LEAVE:
                            monitor_logger.info("Encounter timeout before:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                            self.MembershipList[hostname] = (value[0], utils.Status.LEAVE)
                            monitor_logger.info("Encounter timeout after:")
                            monitor_logger.info(json.dumps(self.MembershipList))
                            for file in self.FILES[hostname]:
                                self.download(GET, file, FILE_DIRECTORY)
                                time.sleep(0.1)
                                self.upload(file, FILE_DIRECTORY)
                            self.FILES[hostname] = []
                            for host in utils.get_all_hosts():
                                with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as m:
                                    time.sleep(0.1)
                                    m.sendto(json.dumps({"COMMAND": HOST_FAIL,  "HOST": hostname}).encode('utf-8'), (host, FILE_PORT))
                        self.last_update.pop(hostname, None)
                
                self.time_lock.release()
            except Exception as e:
                print(e)

    
    def leave(self):
        '''
        Mark current process as LEAVE status

        return: None
        '''
        self.time_lock.acquire()
        prev_timestamp = self.MembershipList[HOST][0]
        monitor_logger.info("Encounter leave before:")
        monitor_logger.info(json.dumps(self.MembershipList))
        self.MembershipList[HOST] = (prev_timestamp, utils.Status.LEAVE)
        monitor_logger.info("Encounter leave after:")
        monitor_logger.info(json.dumps(self.MembershipList))
        print(self.MembershipList)
        self.time_lock.release()

    def print_membership_list(self):
        '''
        Print current membership list
        
        return: None
        '''
        
        print(self.MembershipList)
       
    def print_self_id(self):
        '''
        Print self's id
        
        return: None
        '''
       
        print(IP + "#" + self.MembershipList[HOST][0])
        
    def shell(self):
        print("Welcome to the interactive shell for CS425 MP2. You may press 1/2/3/4 for below functionalities.\n"
              "1. list_mem: list the membership list\n"
              "2. list_self: list self's id\n"
              "3. join: command to join the group\n"
              "4. leave: command to voluntarily leave the group (different from a failure, which will be Ctrl-C or kill)\n"
              "5. grep: get into mp1 grep program"
              )
        
        time.sleep(1)
        # interactive shell
        while True:
            input_str = input("Please enter input: ")
            if input_str == 'exit':
                break
            if input_str == "1":
                print("Selected list_mem")
                self.print_membership_list()
            elif input_str == "2":
                print("Selected list_self")
                self.print_self_id()
            elif input_str == "3":
                print("Selected join the group")
                self.join()
            elif input_str == "4":
                print("Selected voluntarily leave")
                self.leave()
            elif input_str == "5":
                input_command = input("Please enter grep command: ")
                c = mp1_client.Client(input_command)
                t = threading.Thread(target = c.query)
                t.start()
                t.join()
            # file commands
            elif (input_str == "6"):
                self.handle_file()
            elif (input_str == "7"):
                print("Selected file_map")
                for check_host in self.FILES:
                    print("HOST: " + check_host)
                    for file in self.FILES[check_host]:
                        print(file, end=", ")
                    print("\n")
            elif (input_str == "8"):
                for file in self.FILES[HOST]:
                    print(file, end=", ")
                print("\n")
            else:
                print("Invalid input. Please try again")

    def run(self):
        '''
        run function starts the server

        return: None
        '''
        logging.info('Enter run() function.')
        t_monitor = threading.Thread(target=self.monitor_program)
        t_receiver = threading.Thread(target=self.receiver_program)
        t_shell = threading.Thread(target=self.shell)
        t_sender = threading.Thread(target=self.send_ping)
        t_server_mp1 = threading.Thread(target = mp1_server.server_program)
        t_file = threading.Thread(target=self.file_program)
        threads = []
        i = 0
        for host in utils.get_neighbors(HOST):
            t_send = threading.Thread(target=self.send_ping, args=(host, ))
            threads.append(t_send)
            i += 1
        t_monitor.start()
        t_receiver.start()
        t_shell.start()
        t_sender.start()
        t_server_mp1.start()
        t_file.start()
        for t in threads:
            t.start()
        t_monitor.join()
        t_receiver.join()
        t_shell.join()
        t_sender.join()
        t_server_mp1.join()
        t_file.join()
        for t in threads:
            t.join()


if __name__ == '__main__':
    s = Server()
    s.run()
