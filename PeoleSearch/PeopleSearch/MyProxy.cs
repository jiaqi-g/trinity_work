using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Trinity.Data;
using Trinity;
using Trinity.Storage;

using System.Threading.Tasks;
using System.Threading;
using System.Runtime.ExceptionServices;
using System.Security;

namespace PeopleSearch
{
    class MyProxy:MyProxyBase
    {
        static HashSet<long> matches = new HashSet<long>();
        static int report_num = 0;
        static object lock_o = new object();

        //register a handler to deal with query requests from the client
        [HandleProcessCorruptedStateExceptionsAttribute, SecurityCriticalAttribute]
        public override void QueryHandler(NameRequestReader request, ResultWriter responseBuff)
        {
            try
            {
                int slave_count = Global.SlaveCount;
                Global.CloudStorage.SearchToSlave(Global.CloudStorage.GetSlaveIDByCellID(1),
                    new NameRequestWriter(request.hop, request.name, request.neighbours));

                int reports_total = ReportsCount(slave_count);
                Console.WriteLine("Waiting for {0} reports", reports_total);
                //waiting for 40 reports
                while (report_num != reports_total)
                {
                    Thread.Sleep(1000);
                }
                responseBuff.matchPersons = matches.ToList<long>();
                report_num = 0;
                matches.Clear();

            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
        }

        //register a handler to deal with partial result reports from slaves
        public override void ReportHandler(ResultReader request)
        {
            lock (lock_o)
            {
                request.matchPersons.ForEach(match =>
                    {
                        matches.Add(match);
                    }
                );
                Console.WriteLine("This is the {0}th report", ++report_num);
                Console.WriteLine("Now the total match is " + matches.Count);
            }
        }

        private int ReportsCount(int slave_count)
        {
            // a full 3 way tree with depth of 4
            return 1 + slave_count + slave_count * slave_count + slave_count * slave_count * slave_count;
        }
    }
}
