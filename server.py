import socket

def ServerMain():
  HOST = '127.0.0.1'   # use '' to expose to all networks
  PORT = 9999

  s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #create an INET, STREAMing socket

  print ('starting up on %s port %s' % (HOST, PORT) )  
  s.bind((HOST, PORT)) # binds address (hostname, port number pair) to socket.

  s.listen(5) #sets up and start TCP listener, maximum 5 connections

  # wait for a connection
  print ('waiting for a connection')
  conn, addr = s.accept() #passively accept TCP client connection, waiting until connection arrives (blocking)

  while 1:
    # receive data stream
    data  = conn.recv(1024)
    #data = "".join(iter(lambda:conn.recv(1),"\n")) #.recv()receives TCP message
    print ('received',repr(data))
    if not data: break # no more data -- quit the loop

  print('Done Receiving')
  conn.close()
  
  

if __name__ == '__main__':
  ServerMain()