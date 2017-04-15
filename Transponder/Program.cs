using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;

//Transponder program -- Alerts all hosts when a system comes online
//Useful for monitoring when an AWS instance boots up.
namespace Transponder
{

    class Program
    {
        static void Main(string[] args)
        {
            int port = 9090;
            if(args.Any())
            {
                port = int.Parse(args[0]);
                
            }
            UdpClient mclient = new UdpClient(new IPEndPoint(IPAddress.Any, port));
            
            mclient.JoinMulticastGroup(IPAddress.Parse("239.255.255.250"));
            mclient.Send(new byte[1], 1,new IPEndPoint(IPAddress.Parse("239.255.255.250"),9090)); //It's ALIVE!
            
        }
    }
}
