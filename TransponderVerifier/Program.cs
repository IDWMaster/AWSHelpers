using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace TransponderVerifier
{
    class Program
    {
        static void Main(string[] args)
        {
            int port = 9090;
            if (args.Any())
            {
                port = int.Parse(args[0]);

            }
            UdpClient mclient = new UdpClient(new IPEndPoint(IPAddress.Any, port));

            mclient.JoinMulticastGroup(IPAddress.Parse("239.255.255.250"));
            IPEndPoint ep = new IPEndPoint(IPAddress.Any, 0);
            while (true)
            {
                mclient.Receive(ref ep);
            }
        }
    }
}
