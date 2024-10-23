using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Subscriber
{
    class SubscriberSocket
    {
        private Socket _socket;
        private string _topic;

        public SubscriberSocket(string topic)
        {
            _topic = topic;
            _socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        }

        public void Connect(string ipAddress, int port)
        {
            _socket.BeginConnect(new IPEndPoint(IPAddress.Parse(ipAddress), port), ConnectedCallback, null);
            Console.WriteLine("Waiting for a connection");

        }
        private void ConnectedCallback(IAsyncResult asyncResult)
        {
            if (_socket.Connected)
            {
                Console.WriteLine("Subscriber connected to broker");
                Subscriber();
                StartReceive();
            }
            else
            {
                Console.WriteLine("Error: Connection failed. Subscriber couldn't connect to broker");
            }
        }

        private void Subscribe()
        {
            var data = Encoding.UTF8.GetBytes("subscribe#" + _topic);
            Send(data)
        }

        private void StartReceive()
        {
            ConnectionInfo connection = new ConnectionInfo();
            connection.Socket = _socket;
            
            _socket.BeginReceive(connection.Data, 0, connection.Data.Length, SocketFlags.None, ReceiveCallback, connection);
        }

        private void ReceiveCallback(IAsyncResult asyncResult)
        {
            ConnectionInfo connectionInfo = asyncResult.AsyncState as connectionInfo;

            try
            {
                SocketError response;
                int buffSize = _socket.EndReceive(asyncResult, out response);

                if (response == SocketError.Success)
                {
                    byte[], payloadBytes = new byte[buffSize];
                    Array.Copy(connectionInfo.Data, payloadBytes, payloadBytes.Length);

                    PayloadHandler.Handle(payloadBytes);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine($"Can't receive date from broker. {e.Message}");

            }
            finally
            {
                try
                {
                    connectionInfo.Socket.BeginReceive(connectionInfo.Data, 0, connectionInfo.Data.Lenght, SocketFlags.None, ReceiveCallback, connectionInfo);
                }
                catch (Exception e)
                {
                    Console.WriteLine($"{e.Message}");
                    connectionInfo.Socket.Close();
                }
            }
              

        }

        private void Send(byte[] data)
        {
            try
            {
                _socket.Send(data);
            }
            catch(Exception e) 
            { 
                Console.WriteLine($"Couldn't send data: {e.Message}"); 
            }
        }

    }
}
