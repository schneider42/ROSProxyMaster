# Software License Agreement (BSD License)
#
# Copyright (c) 2012, Fraunhofer FKIE/US, Alexander Tiderko
# All rights reserved.
# Tobias Schneider - schneider.tobias@mytum.de
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  * Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#  * Redistributions in binary form must reproduce the above
#    copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided
#    with the distribution.
#  * Neither the name of I Heart Engineering nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from udp import McastSocket
import time
import threading
import xmlrpclib
import copy
import sys
import socket
import time
import struct
import select
from urlparse import urlparse

class Discoverer():
  '''
  The class to publish the current state of the ROS master.
  '''

  VERSION = 1
  '''@ivar: the version of the packet format described by L{HEARTBEAT_FMT}'''
  '''
  Version 1:
  '>cBH':
      >: Network byte order
      char: One character 'A'
      uint8_t: Version of the hearbeat message.
      uint16_t: Interval of the heartbeat message in ms.
      uint32_t: Version of the current state.
  RPC URL of the local master.
  '''
  HEARTBEAT_FMT = '>cBHI'
  ''' @ivar: packet format description, see: U{http://docs.python.org/library/struct.html} '''
  HEARTBEAT_INTERVAL = 5000
  ''' @ivar: the send rate of the heartbeat packets in hz '''
  MEASUREMENT_INTERVALS = 5  
  ''' @ivar: the count of intervals (1 sec) used for a quality calculation. If HEARTBEAT_HZ is smaller then 1, MEASUREMENT_INTERVALS will be divided by HEARTBEAT_HZ value '''
  TIMEOUT_FACTOR = 1.4       
  ''' @ivar: the timeout is defined by calculated measurement duration multiplied by TIMEOUT_FAKTOR. ''' 


class ProxyMaster(threading.Thread):
  def __init__(self, mcast_port, mcast_group):
    threading.Thread.__init__(self)
    self.msocket = msocket = McastSocket(mcast_port, mcast_group)
    if not self.msocket.hasEnabledMulticastIface():
      sys.exit("No enabled multicast interfaces available!\nAdd multicast support e.g. sudo ifconfig eth0 multicast")
    
    self.__lock = threading.RLock()
    # the list with all ROS master neighbors
    self.masters = dict() # (ip, DiscoveredMaster)

    # create a thread to handle the received multicast messages
    self._recvThread = threading.Thread(target = self.recv_loop)
    self._recvThread.setDaemon(True)
    self._recvThread.start()

    self.proxy_socket = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
    self.proxy_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    self.proxy_socket.bind(('::', 11311))
    self.proxy_socket.listen(5)

#    # create a timer monitor the offline ROS master and calculate the link qualities
#    try:
#      self._statsTimer = threading.Timer(1, self.timed_stats_calculation)
#      self._statsTimer.start()
#    except:
#      print "ROS Timer is not available! Statistic calculation and timeouts are deactivated!"


  def recv_loop(self):
    '''
    This method handles the received multicast messages.
    '''
    structsize = struct.calcsize(Discoverer.HEARTBEAT_FMT)
    while self.msocket: #and not self.do_finish:
      try:
        (msg, address) = self.msocket.recvfrom(1024)
      except socket.timeout:
#        rospy.logwarn("TIMOUT ignored")
        pass
      except socket.error:
        import traceback
        print "socket error: %s", traceback.format_exc()
      else:
        if len(msg) > 2:
          (r,) = struct.unpack('c', msg[0])
          (version,) = struct.unpack('B', msg[1])
          if (version == Discoverer.VERSION):
            if (r == 'A'):
                (r, version, interval, version) = struct.unpack(Discoverer.HEARTBEAT_FMT, msg[:structsize])
                masterrpc = msg[structsize:]
                #TODO: validate url

                # remove master if interval is 0
                if interval == 0:
                  if self.masters.has_key(masterrpc):
                    self.__lock.acquire(True)
                    del self.masters[masterrpc]
                    self.__lock.release()
                # update the timestamp of existing master
                elif self.masters.has_key(masterrpc):
                  self.__lock.acquire(True)
                  self.masters[masterrpc]['version']=version
                  #self.masters[address[0]].addHeartbeat(secs, float(rate)/10.0)
                  self.__lock.release()
                # or create a new master
                else:
                  print "create new master", masterrpc, version
                  self.__lock.acquire(True)
                  self.masters[masterrpc] = {'masterrpc': masterrpc, 'version': version}
                  self.__lock.release()
            else:
              print "wrong initial discovery message char %s received from %s ", str(r), str(address)
          elif (msgversion > Discoverer.VERSION):
            print "newer heartbeat version %s (own: %s) detected, please update your master_discovery", str(version), str(Discoverer.VERSION)
          else:
            print "old heartbeat version %s detected (current: %s), please update master_discovery on %s", str(version), str(Discoverer.VERSION), str(address)
  
  def run(self):
    while True:
        time.sleep(1)
        with self.__lock:
            for m in self.masters.values():
                pass #print m.quality
  
  def serve(self):
    open_sockets = []
    client_connections = {}
    server_connections = {}

    while True:
        rlist, wlist, xlist = select.select( [self.proxy_socket] + open_sockets, [], [])
        for s in rlist:
            if s is self.proxy_socket:
                server_connection = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
                client_connection, addr = self.proxy_socket.accept()
                print 'new connection from', addr
                master = self.getPreferedMaster()
                if master is not None:
                    print 'opening connection to', master
                    masteruri = urlparse(master['masterrpc'])
                    server_connection.connect((masteruri.hostname, masteruri.port))
                    client_connections[client_connection] = server_connection;
                    server_connections[server_connection] = client_connection;
                    
                    open_sockets.append(server_connection)
                    open_sockets.append(client_connection)
                else:
                    print 'no master available'
                    client_connection.close()

            elif s in client_connections:
                data = s.recv(1024)
                if len(data) > 0:
                    client_connections[s].sendall(data)
                else:
                    print 'client closed the connection'
                    client_connections[s].close()
                    s.close()
                    open_sockets.remove(s)
                    open_sockets.remove(client_connections[s])
                    del server_connections[client_connections[s]]
                    del client_connections[s]

            elif s in server_connections:
                data = s.recv(1024)
                if len(data) > 0:
                    server_connections[s].sendall(data)
                else:
                    print 'server closed the connection'
                    server_connections[s].close()
                    s.close()
                    open_sockets.remove(s)
                    open_sockets.remove(server_connections[s])
                    del client_connections[server_connections[s]]
                    del server_connections[s]



  def getPreferedMaster(self):
    with self.__lock:
        #maxq = 0
        selected = None

        for m in self.masters:
            master = self.masters[m]
            if master['masterrpc'] != None:
            #and master.quality >= maxq:
                selected = master
                #maxq = master.quality
        return selected 

pm = ProxyMaster(11511, '226.0.0.0')
pm.serve()
#pm.run()
#pm.join()

