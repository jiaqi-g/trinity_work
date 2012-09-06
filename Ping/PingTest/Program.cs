using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Trinity.Data;
using Trinity.Storage;
using Trinity;
using System.Threading;

namespace PingTest
{
    class MySlave : MySlaveBase
    {
        public override void SynPingHandler(MyMessageReader request)
        {
            Console.WriteLine("Received SynPing, sn={0}", request.sn);
        }

        public override void AsynPingHandler(MyMessageReader request)
        {
            Console.WriteLine("Received AsynPing, sn={0}", request.sn);
        }

        public override void SynEchoPingHandler(MyMessageReader request,
        MyMessageWriter response)
        {
            Console.WriteLine("Received SynEchoPing, sn={0}", request.sn);
            response.sn = request.sn;
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var server = new MySlave();
            server.Start(false);

            var synReq = new MyMessageWriter(1);

            Global.CloudStorage.SynPingToSlave(0, synReq);

            var asynReq = new MyMessageWriter(2);
            Global.CloudStorage.AsynPingToSlave(0, asynReq);

            var synReqRsp = new MyMessageWriter(3);
            Console.WriteLine("response: " + Global.CloudStorage.SynEchoPingToSlave(0, synReqRsp).sn);

            while (true)
            {
                Thread.Sleep(3000);
            }

        }
    }
}
