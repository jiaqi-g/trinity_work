using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Trinity;
using Trinity.Storage;
using Trinity.Data;
using Trinity.Network.Messaging;
using System.Threading;

namespace SSSP
{
    class Msg {
        
        public long sender_id;
        public long recipient_id;
        public double weight;

        public Msg(long sender_id, long recipient_id, double weight)
        {
            this.sender_id = sender_id;
            this.recipient_id = recipient_id;
            this.weight = weight;
        }
    }

    class SSSPSlave : SSSPSlaveBase
    {
        public static TextWriter writer = File.CreateText("log");
        //between 0-1
        private long node_count = 0;
        public String suffix = "undefined";
        public double precision = 0;
        
        public const double baseline = 1.0;
        public static int i = 0;

        public static long numOfMessage = 0;
        public static double level = 0.0001;

        public static void addCount()
        {
            Interlocked.Increment(ref numOfMessage);
        }

        public static long returnCount()
        {
            lock (typeof(SSSPSlave))
            {
                return numOfMessage;
            }
        }

        public static double returnLevel()
        {
            lock (typeof(SSSPSlave))
            {
                return level;
            }
        }
        
        /*
         * write to console is automatically activated
         */
		private void writeToFile(String line)
        {
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"C:\Users\Administrator\Desktop\output\PR_Datalog_" + suffix, true))
            {
                Console.WriteLine(line);
                file.WriteLine(line);
            }
        }

        private static int binarySearch(List<long> list, long key)
        {
            int left = 0;
            int right = list.Count - 1;
            while (left <= right)
            {
                int middle = (left + right) / 2;
                if (list[middle] > key)
                {
                    right = middle - 1;
                }
                else if (list[middle] < key)
                {
                    left = middle + 1;
                }
                else
                {
                    return middle;
                }
            }

            throw new Exception("key not found by binarySearch");
        }

        [System.Runtime.ExceptionServices.HandleProcessCorruptedStateExceptionsAttribute, System.Security.SecurityCriticalAttribute]
        public override void RankUpdatingHandler(RankUpdatingMessageReader request)
        {
            request.recipients.ForEach((recipient) =>
            {
                using (var cell = Global.LocalStorage.UseSSSPCell(recipient))
                {
                    int index = binarySearch(cell.inNeighbors, request.senderId);

                    if (cell.ranks[index] < request.value)
                    {
                        cell.sum += request.value - cell.ranks[index];
                        cell.ranks[index] = request.value;

                        if (cell.CellID == 3)
                        {
                            Console.WriteLine(cell.sum);
                        }

                        send(cell);
                    }
                }
            });
        }

        private void send(SSSPCell rootCell)
        {
            //Console.WriteLine(value);
            addCount();

            double v = rootCell.sum / rootCell.outNeighbors.Count;

            MessageSorter sorter = new MessageSorter(rootCell.outNeighbors);
            for (int i = 0; i < Global.SlaveCount; i++)
            {
                RankUpdatingMessageWriter msg =
                    new RankUpdatingMessageWriter(rootCell.CellID, v, sorter.GetCellRecipientList(i));
                Global.CloudStorage.RankUpdatingToSlave(i, msg);
            }
        }

        private void initialSend(SSSPCell rootCell)
        {
            //Console.WriteLine(value);
            addCount();

            double v = 1 / rootCell.outNeighbors.Count;

            MessageSorter sorter = new MessageSorter(rootCell.outNeighbors);
            for (int i = 0; i < Global.SlaveCount; i++)
            {
                RankUpdatingMessageWriter msg =
                    new RankUpdatingMessageWriter(rootCell.CellID, v, sorter.GetCellRecipientList(i));
                Global.CloudStorage.RankUpdatingToSlave(i, msg);
            }
        }
        
        public override void StartSSSPHandler(StartSSSPMessageReader request)
        {
            //Console.SetOut(writer);
            //get info
            //thresholdFactor = request.thresholdFactor;
            precision = request.precision;
            node_count = request.node_count;
            suffix = request.node_count + "_" + precision + ".txt";

            //
            writeToFile("Node_count:\t" + node_count + "\tPrecision:\t" + precision);
            writeToFile("Iteration\tTime(ms)\tActived_Node_Count");
            
            //doit
            //parseFile("C:\\Users\\Administrator\\Desktop\\b.txt");
            for (long k = 0; k < request.node_count; k++)
            {
                if (Global.CloudStorage.Contains(k) && Global.CloudStorage.IsLocalCell(k))
                {
                    //!!! UseSSSPCell must be wrapped in using clause
                    using (var cell = Global.LocalStorage.UseSSSPCell(k))
                    {
                        initialSend(cell);
                    }
                }
            }

            long old = -1;
            long i = 0;

            DateTime dt = DateTime.Now;
            while (true)
            {
                Thread.Sleep(1000);

                //if (i % 100000 == 0)
                //{
                    //Console.WriteLine(returnCount());
                    long d = returnCount();
                    //Console.WriteLine(d + " " + old + " " + returnLevel());

                    
                    if (d == old)
                    {
                        TimeSpan ts = DateTime.Now - dt;
                        Console.WriteLine("Time: " + ts.TotalMilliseconds.ToString());
                        break;
                    }

                    //changeLevel(d, old);
                    old = returnCount();
                    
                    
                    //Console.WriteLine(old);
                //}
            }

            //writeToFile("End!------");
            //print(nodes);
        }
        
    }

    class Node
    {
        public long node_id;
        public List<long> inNeighbors = new List<long>();
        public List<long> outNeighbors = new List<long>();

        public Node(int node_id)
        {
            this.node_id = node_id;
        }

        public void addInNeighbor(Node node)
        {
            inNeighbors.Add(node.node_id);
        }

        public void addOutNeighbor(Node node)
        {
            outNeighbors.Add(node.node_id);
        }

        public void sortNeighbors()
        {
            inNeighbors.Sort();
            outNeighbors.Sort();
        }
    }

    class Program
    {
        static void loadFile(String path, int nodes_count)
        {
            Node[] nodes = new Node[nodes_count];
            for (int i = 0; i < nodes_count; i++)
            {
                nodes[i] = new Node(i);
            }

            using (StreamReader sr = new StreamReader(path))
            {
                String line = null; //= sr.ReadLine();

                while ((line = sr.ReadLine()) != null)
                {
                    String[] array = line.Split('\t');

                    int cellId = int.Parse(array[0]);
                    int neighborId = int.Parse(array[1]);

                    nodes[cellId].addOutNeighbor(nodes[neighborId]);
                    nodes[neighborId].addInNeighbor(nodes[cellId]);
                }
            }

            foreach (Node node in nodes)
            {
                node.sortNeighbors();
                List<double> ranks = new List<double>(node.inNeighbors.Count);
                for (int i = 0; i < node.inNeighbors.Count; ++i)
                {
                    ranks.Add(0);
                }

                Global.CloudStorage.SaveSSSPCell(node.node_id, inNeighbors: node.inNeighbors, outNeighbors: node.outNeighbors, ranks: ranks);
            }
        }
        
        static void Main(string[] args)
        {
            if (args.Length >= 1 && args[0].StartsWith("-s"))
            {
                SSSPSlave slave = new SSSPSlave();
                slave.Start(true);
            }

            //SSSP.exe -c node_count iteration_time precision
            if (args.Length >= 4 && args[0].StartsWith("-c"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;

                long node_count = long.Parse(args[1].Trim());
                long iteration_time = long.Parse(args[2].Trim());
                double precision = double.Parse(args[3].Trim());

                //DateTime dt = DateTime.Now;
                for (int i = 0; i < Global.SlaveCount; i++)
                {
                    Global.CloudStorage.StartSSSPToSlave(i, new StartSSSPMessageWriter(node_count, precision));
                    //Global.CloudStorage.ChangeSSSPToSlave(i, new ChangeSSSPMessageWriter(node_count));
                }
                //TimeSpan ts = DateTime.Now - dt;
                //Console.WriteLine("All Time --- " + " Node_count: " + node_count + " Time: " + ts.TotalMilliseconds.ToString());
            }

            //SSSP.exe -q cellID
            if (args.Length >= 2 && args[0].StartsWith("-q"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                var cell = Global.CloudStorage.LoadSSSPCell(int.Parse(args[1]));
                Console.Write("Current Node's id is {0}, The RankValue is {1}, The neighbours are",
                    cell.CellID, Math.Max(cell.sum, 1));

                Console.WriteLine();
                
            }

            //SSSP.exe -g node_count
            /*
            if (args.Length >= 2 && args[0].StartsWith("-g"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;

                Random rand = new Random();
                int nodeCount = int.Parse(args[1].Trim());

                for (int i = 0; i < nodeCount; i++)
                {
                    HashSet<long> neighbors = new HashSet<long>();
                    for (int j = 0; j < 10; j++)
                    {
                        long neighor = rand.Next(0, nodeCount);
                        if (neighor != i)
                        {
                            neighbors.Add(neighor);
                        }
                    }
                    Global.CloudStorage.SaveSSSPCell(i, rankValue: INIRANK, status: true, neighbors: neighbors.ToList());
                }
            }
            */

            //SSSP.exe -r fileName node_count
            //search in the desktop
            if (args.Length >= 3 && args[0].StartsWith("-r"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                String path = "C:\\Users\\Administrator\\Desktop\\" + args[1];
                loadFile(path, int.Parse(args[2]));
            }

        }
    }
}