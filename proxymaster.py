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

STATE_NEW='new'
STATE_REMOVED='removed'
STATE_CHANGED='changed'

class DiscoveredMaster(object):
  '''
  The class stores all information about the remote ROS master and the all
  received heartbeat messages of the remote node. On first contact a theaded 
  connection to remote discoverer will be established to get additional 
  information about the ROS master.
  '''
  def __init__(self, monitoruri, heartbeat_rate=1., timestamp=0.0, callback_master_state=None):
    '''
    Initialize method for the DiscoveredMaster class.
    @param monitoruri: The URI of the remote RPC server, which moniter the ROS master
    @type monitoruri:  C{str}
    @param heartbeat_rate: The remote rate, which is used to send the heartbeat messages. 
    @type heartbeat_rate:  C{float} (Default: C{1.})
    @param timestamp: The timestamp of the state of the remoter ROS master
    @type timestamp:  C{float} (Default: c{0})
    @param callback_master_state: the callback method to publish the changes of the ROS masters
    @type callback_master_state: C{<method>(master_discovery_fkie/MasterState)}  (Default: C{None})
    '''
    self.masteruri = None
    self.mastername = None
    self.timestamp = timestamp
    self.discoverername = None
    self.monitoruri = monitoruri
    self.heartbeat_rate = heartbeat_rate
    self.heartbeats = list()
    self.online = False
    self.quality = 0
    self.callback_master_state = callback_master_state
    # create a thread to retrieve additional information about the remote ROS master
    self._retrieveThread = threading.Thread(target = self.__retrieveMasterinfo)
    self._retrieveThread.setDaemon(True)
    self._retrieveThread.start()

  def addHeartbeat(self, timestamp, rate):
    '''
    Adds a new heartbeat measurement. If it is a new timestamp a ROS message 
    about the change of this ROS master will be published into ROS network.
    @param timestamp: The new timestamp of the ROS master state
    @type timestamp:  C{float}
    @param rate: The remote rate, which is used to send the heartbeat messages. 
    @type rate:  C{float}
    '''
    self.heartbeats.append(time.time())
    # reset the list, if the heartbeat is changed
    if self.heartbeat_rate != rate:
      self.heartbeat_rate = rate
      self.heartbeats = list()
    # publish new master state, if the timestamp is changed 
    if (self.timestamp != timestamp or not self.online):
      self.timestamp = timestamp
      if not (self.masteruri is None):
        #set the state to 'online'
        self.online = True
        if not (self.callback_master_state is None):
          self.callback_master_state((STATE_CHANGED, 
                                                 (str(self.mastername), 
                                                           self.masteruri, 
                                                           self.timestamp, 
                                                           self.online, 
                                                           self.discoverername, 
                                                           self.monitoruri)))

  def removeHeartbeats(self, timestamp):
    '''
    Removes all hearbeat measurements, which are older as the given timestamp.
    @param timestamp: heartbeats older this timestamp will be removed.
    @type timestamp:  C{float}
    @return: the count of removed heartbeats
    @rtype: C{int}
    '''
    do_remove = True
    removed = 0
    while do_remove:
      if len(self.heartbeats) > 0 and self.heartbeats[0] < timestamp:
        del self.heartbeats[0]
        removed = removed + 1
      else:
        do_remove = False
    return removed

  def setOffline(self):
    '''
    Sets this master to offline and publish the new state to the ROS network.
    '''
    self.online = False
    if not (self.callback_master_state is None):
      self.callback_master_state((STATE_CHANGED, 
                                             (str(self.mastername), 
                                                       self.masteruri, 
                                                       self.timestamp, 
                                                       self.online, 
                                                       self.discoverername, 
                                                       self.monitoruri)))

  def __retrieveMasterinfo(self):
    '''
    Connects to the remote RPC server of the discoverer node and gets the 
    information about the Master URI, name of the service, and other. The 
    getMasterInfo() method will be used. On problems the connection will be 
    reestablished until the information will be get successful.
    '''
    if not (self.monitoruri is None):
      #while self._retrieveThread.is_alive() and not rospy.is_shutdown() and (self.mastername is None):
      while self._retrieveThread.is_alive() and (self.mastername is None):
        try:
          print "get Info about master", self.monitoruri
          remote_monitor = xmlrpclib.ServerProxy(self.monitoruri)
          timestamp, masteruri, mastername, nodename, monitoruri = remote_monitor.masterContacts()
        except:
          import traceback
          print "socket error: %s", traceback.format_exc()
          time.sleep(1)
        else:
          if float(timestamp) != 0:
            self.masteruri = masteruri
            self.mastername = mastername
            self.discoverername = nodename
            self.monitoruri = monitoruri
            self.timestamp = float(timestamp)
            self.online = True
            #publish new node 
            if not (self.callback_master_state is None):
              self.callback_master_state((STATE_NEW, 
                                                     (str(self.mastername), 
                                                               self.masteruri, 
                                                               self.timestamp, 
                                                               self.online, 
                                                               self.discoverername, 
                                                               self.monitoruri)))
          else:
            time.sleep(1)





class Discoverer():
  '''
  The class to publish the current state of the ROS master.
  '''

  VERSION = 1
  '''@ivar: the version of the packet format described by L{HEARTBEAT_FMT}'''
  '''
  Version 1: 'cBBiiH'
    one character 'R'
    unsigned char: version of the hearbeat message
    unsigned char: rate of the heartbeat message in HZ*10. Maximal rate: 25.5 Hz -> value 255
    int: secs of the ROS Master state
    int: nsecs of the ROS Master state
    unsigned short: the port number of the RPC Server of the remote ROS-Core monitor
  '''
  HEARTBEAT_FMT = 'cBBiiH'
  ''' @ivar: packet format description, see: U{http://docs.python.org/library/struct.html} '''
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

    # create a timer monitor the offline ROS master and calculate the link qualities
    try:
      self._statsTimer = threading.Timer(1, self.timed_stats_calculation)
      self._statsTimer.start()
    except:
      print "ROS Timer is not available! Statistic calculation and timeouts are deactivated!"


  def recv_loop(self):
    '''
    This method handles the received multicast messages.
    '''
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
            if (r == 'R'):
              if len(msg) == struct.calcsize(Discoverer.HEARTBEAT_FMT):
                (r, version, rate, secs, nsecs, monitor_port) = struct.unpack(Discoverer.HEARTBEAT_FMT, msg)
                # remove master if sec and nsec are -1
                if secs == -1:
                  if self.masters.has_key(address[0]):
                    master = self.masters[address[0]]
                    #if not master.mastername is None:
                    #  self.publish_masterstate(MasterState(MasterState.STATE_REMOVED, 
                    #                                 ROSMaster(str(master.mastername), 
                    #                                           master.masteruri, 
                    #                                           master.timestamp, 
                    #                                           False, 
                    #                                           master.discoverername, 
                    #                                           master.monitoruri)))
                    self.__lock.acquire(True)
                    del self.masters[address[0]]
                    self.__lock.release()
                # update the timestamp of existing master
                elif self.masters.has_key(address[0]):
                  self.__lock.acquire(True)
                  self.masters[address[0]].addHeartbeat(secs, float(rate)/10.0)
                  self.__lock.release()
                # or create a new master
                else:
                  print "create new masterstate", ''.join(['http://', address[0],':',str(monitor_port)])
                  self.__lock.acquire(True)
                  self.masters[address[0]] = ''.join(['http://', address[0],':',str(monitor_port)])
                  self.masters[address[0]] = DiscoveredMaster(monitoruri=''.join(['http://', address[0],':',str(monitor_port)]), 
                                                              heartbeat_rate=float(rate)/10.0,
                                                              timestamp=float(secs)+float(nsecs)/1000000000,
                                                              callback_master_state=self.changed_masterstate)
                  self.__lock.release()
            else:
              print "wrong initial discovery message char %s received from %s ", str(r), str(address)
          elif (version > Discoverer.VERSION):
            print "newer heartbeat version %s (own: %s) detected, please update your master_discovery", str(version), str(Discoverer.VERSION)
          elif (version < Discoverer.VERSION):
            print "old heartbeat version %s detected (current: %s), please update master_discovery on %s", str(version), str(Discoverer.VERSION), str(address)
          else:
            print "heartbeat version %s expected, received: %s", str(Discoverer.VERSION), str(version)
  
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
                if len(self.masters) > 0 and self.masters.values()[0].masteruri is not None:
                    print 'opening connection to', self.masters.values()[0].masteruri
                    master = urlparse(self.masters.values()[0].masteruri)
                    server_connection.connect((master.hostname, master.port))
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



  def timed_stats_calculation(self):
    '''
    This method will be called by a timer and has two jobs:
     1. set the masters offline, if no heartbeat messages are received a long time
     2. calculate the quality of known links
    @see: L{float}
    '''
    #result = LinkStatesStamped()
    current_time = time.time()
    #result.header.stamp.secs = int(current_time)
        #result.header.stamp.nsecs = int((current_time - result.header.stamp.secs) * 1000000000)
    try:
      self.__lock.acquire(True)
  #    self.__lock.release()
      for (k, v) in self.masters.iteritems():
        quality = -1.0
   #     self.__lock.acquire(True)
        if not (v.mastername is None):
          rate = v.heartbeat_rate
          measurement_duration = Discoverer.MEASUREMENT_INTERVALS
          if rate < 1.:
            measurement_duration = Discoverer.MEASUREMENT_INTERVALS / rate
          # remove all heartbeats, which are to old
          ts_oldest = current_time - measurement_duration
          removed_ts = v.removeHeartbeats(ts_oldest)
          # sets the master offline if the last received heartbeat is to old
          if len(v.heartbeats) > 0:
            last_ts = v.heartbeats[-1]
            if current_time - last_ts > measurement_duration * Discoverer.TIMEOUT_FACTOR:
              v.setOffline()
          elif removed_ts > 0: # no heartbeats currently received, and last removed, so set master offline
            v.setOffline()
          # calculate the quality for inly online masters
          if v.online:
            beats_count = len(v.heartbeats)
            expected_count = rate * measurement_duration
    #        print "beats_count:",beats_count, ", expected_count:",expected_count
            if expected_count > 0:
              quality = float(beats_count) / float(expected_count) * 100.0
              if quality > 100.0:
                quality = 100.0
            v.quality = quality
            #result.links.append(LinkState(v.mastername, quality))
    finally:
      self.__lock.release()
    #publish the results
    #self.publish_stats(result)
    try:
    #  if not rospy.is_shutdown():
      self._statsTimer = threading.Timer(1, self.timed_stats_calculation)
      self._statsTimer.start()
    except:
      pass


  def changed_masterstate(self, master_state):
    '''
    Publishes the given state to the ROS network. This method is thread safe.
    @param master_state: the master state to publish
    @type master_state:  L{master_discovery_fkie.MasterState}
    '''
    if self.__lock.acquire(True):
      try:
        print master_state
      except:
        import traceback
        traceback.print_exc()
      finally:
        self.__lock.release()

pm = ProxyMaster(11511, '226.0.0.0')
pm.serve()
#pm.run()
#pm.join()

