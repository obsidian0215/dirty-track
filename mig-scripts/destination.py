#!/usr/bin/env python
#code retrieved from https://www.redhat.com/en/blog/container-migration-around-world and partially modified
import socket
import sys
from _thread import *
import json
import os
import shutil
import distutils.util
import time
import subprocess

def prepare(image_path, parent_path):
    try:
        shutil.rmtree(image_path)
    except:
        pass
    try:
        shutil.rmtree(parent_path)
    except:
        pass
    os.mkdir(parent_path)
    os.mkdir(image_path)

def migrate_server():
    HOST = ''   # Symbolic name meaning all available interfaces
    PORT = 18863

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print('Socket created')

    #Bind socket to local host and port
    try:
        s.bind((HOST, PORT))
    except socket.error as msg:
        print('Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1])
        sys.exit()

    print('Socket bind complete')

    #Start listening on socket
    s.listen(10)
    print('Socket now listening')

    #Function for handling connections. This will be used to create threads
    def clientthread(conn, addr):
        #Sending message to connected client

        #infinite loop so that function does not terminate and thread does not end.
        while True:

            reply = ""
            #Receiving from client
            data = conn.recv(1024)
            #print(data)
            if not data:
                break
            if data == 'exit':
                break

            try:
                #Parse JSON string into Python dictionary
                msg = json.loads(data)
                print(msg)
                
                old_cwd = os.getcwd()

                match msg:
                    case {'pageserver':_}:
                        #os.system('criu -V')
                        mount_cmd = 'mount -t tmpfs none ' + msg['pageserver']['precopy_path']
                        umount_cmd = 'umount ' + msg['pageserver']['precopy_path']

                        terminate = False
                        if msg['pageserver'] == 'terminate':
                            terminate = True
                        
                        if terminate == True:
                            #cmd = 'killall criu page-server'
                            #print ("Killing page server: " + cmd)
                            #os.system(cmd)
                            print ("Killing page server")
                            ps.terminate()
                            os.system(umount_cmd)
                            continue
                        else:
                            print("start page server")
                            os.system(mount_cmd)

                            cmd = 'criu page-server --images-dir ' + msg['pageserver']['precopy_path']
                            cmd += ' --port 27 -vv -o ' + old_cwd + '/logs/ps.log'
                            print ("Running page server for pre-copy: " + cmd)
                            ps = subprocess.Popen(cmd, shell=True)
                            exitcode = ps.poll()
                            print(exitcode)
                            if exitcode is not None:
                                reply = 'remote criu page-server failed'
                            else:
                                continue
                
                    case {'prepare':_}:
                        path = msg['prepare']['path']
                        image_path = msg['prepare']['image_path']
                        parent_path = msg['prepare']['parent_path']
                        path_exist = os.path.exists(path)
                        if not path_exist:
                            reply = 'cannot find corresponding container bundle'
                        else:
                            try:
                                umount_cmd = 'umount ' + parent_path
                                os.system(umount_cmd)
                            except:
                                pass
                            prepare(image_path, parent_path)
                            continue

                    case {'restore':_}:
                        os.system('criu -V')

                        try:
                            lazy = bool(distutils.util.strtobool(msg['restore']['lazy']))
                            tty = bool(distutils.util.strtobool(msg['restore']['shell-job']))
                            netdump = bool(distutils.util.strtobool(msg['restore']['tcp-established']))
                        except:
                            lazy = False

                        old_cwd = os.getcwd()
                        os.chdir(msg['restore']['path'])
                        #The following command is the restore command, which resotres execution of the container at destination
                        cmd = 'time -p runc restore --console-socket ' + msg['restore']['path']
                        cmd += '/console.sock -d --image-path ' + msg['restore']['image_path']
                        cmd += ' --work-path ' + msg['restore']['image_path']
                        if tty:
                            cmd += ' --shell-job'
                        if netdump:
                            cmd += ' --tcp-established'
                        #In case of a post-copy phase in the migration technique, the restore command restores the process without filling out the entire memory contents.
                        #When the --lazy-pages option is used, restore registers the lazy virtual memory areas (VMAs) with the userfaultfd mechanism. The lazy pages are completely handled by dedicated lazy-pages daemon.
                        #The daemon receives userfault file descriptors from restore via UNIX socket.
                        if lazy:
                            cmd += ' --lazy-pages'
                            #This new command starts the lazy-pages daemon. The daemon monitors the UFFD events and repopulates the tasks address space by requesting lazy pages to the page server running on the source.
                            #Please, read https://criu.org/CLI/opt/--lazy-pages and https://criu.org/Userfaultfd for more information.
                            #The daemon tracks and prints the flow of time and clearly prints when it starts requesting faulted pages and when it finishes, along with an indication of the number of transferred faulted pages.
                            #Note that each page is 4KB.                            
                            lazy_cmd = "criu lazy-pages --page-server --address " + addr
                            lazy_cmd += " --port 27 -vv -D "
                            lazy_cmd += msg['restore']['image_path']
                            lazy_cmd += " -W "
                            lazy_cmd += msg['restore']['image_path']
                            lazy_cmd += " -o logs/lp.log"
                            print ("Running lazy-pages server: " + lazy_cmd)
                            lp = subprocess.Popen(lazy_cmd, shell=True)
                        cmd += ' ' + msg['restore']['name']
                        print("Running " +  cmd)
                        start = time.perf_counter() * 1000
                        p = subprocess.Popen(cmd, shell=True)
                        # if lazy:
                        #     lazy_cmd = "criu lazy-pages --page-server --address " + addr
                        #     lazy_cmd += " --port 27 -vv -D "
                        #     lazy_cmd += msg['restore']['image_path']
                        #     lazy_cmd += " -W "
                        #     lazy_cmd += msg['restore']['image_path']
                        #     lazy_cmd += " -o logs/lp.log"
                        #     print ("Running lazy-pages server: " + lazy_cmd)
                        #     lp = subprocess.Popen(lazy_cmd, shell=True)
                        ret = p.wait()
                        end = time.perf_counter() * 1000
                        if ret == 0:
                            reply = "runc restored %s successfully with %.3f ms" % (msg['restore']['name'], end - start)
                        else:
                            reply = "runc failed(%d)" % ret
                        os.chdir(old_cwd)
                    case _:
                        print("Unknown request: " + msg)
                        reply = 'unknown request'
            except:
                continue

            print(reply)
            conn.sendall(bytes(reply, encoding='utf-8'))

        #came out of loop
        conn.close()

    #now keep talking with the client
    while 1:
        #wait to accept a connection - blocking call
        conn, addr = s.accept()
        print('Connected with ' + addr[0] + ':' + str(addr[1]))

        #start new thread takes 1st argument as a function name to be run, second is the tuple of arguments to the function.
        start_new_thread(clientthread,(conn, str(addr[0]),))

    s.close()

if __name__ == '__main__':
    migrate_server()
