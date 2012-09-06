using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Trinity;
using Trinity.Storage;
using Trinity.Data;
using Trinity.Network.Messaging;

namespace SSSP
{
    class SSSPSlave : SSSPSlaveBase
    {
        private int unConvergedNodeCount = 0;
        private double thresholdFactor = 0;

        private string suffix = "undefined";

        public override void DistanceUpdatingHandler(DistanceUpdatingMessageReader request)
        {
            //List<DistanceUpdatingMessage> DistanceUpdatingMessageList = new List<DistanceUpdatingMessage>();
            request.recipients.ForEach((cellId) =>
                {
                    using (var cell = Global.LocalStorage.UseSSSPCell(cellId))
                    {
                        cell.newRankValue += request.rankValueP;

                        //Console.WriteLine(cell.CellID.Value + ": " + cell.newRankValue);
                        
                        /*
                        MessageSorter sorter = new MessageSorter(cell.neighbors);
                        for (int i = 0; i < Global.SlaveCount; i++)
                        {
                                DistanceUpdatingMessageWriter msg = new DistanceUpdatingMessageWriter(cell.CellID.Value,
                                    cell.distance, sorter.GetCellRecipientList(i));
                                Global.CloudStorage.DistanceUpdatingToSlave(i, msg);
                        }
                        */
                    }
                });
        }

        /*
         * write to console is automatically activated
         */
        private void writeToFile(String line)
        {
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"C:\Users\Administrator\Desktop\output\PR_Origin_" + suffix, true))
            {
                Console.WriteLine(line);
                file.WriteLine(line);
            }
        }

        private void step_two(long node_count)
        {
            unConvergedNodeCount = 0;

            for (int k = 0; k < node_count; k++)
            {
                if (Global.CloudStorage.Contains(k) && Global.CloudStorage.IsLocalCell(k))
                {
                    using (var rootCell = Global.LocalStorage.UseSSSPCell(k))
                    {
                        if (rootCell.neighbors.Count == 0) continue;

                        double diff = Math.Abs(rootCell.newRankValue - rootCell.rankValue);

                        if (diff > thresholdFactor * rootCell.rankValue)
                        {    
                            unConvergedNodeCount++;
                        }
                        rootCell.rankValue = rootCell.newRankValue;
                        rootCell.newRankValue = 0;
                    }
                }
            }
        }

        private void step_one(long node_count)
        {
            for (int k = 0; k < node_count; k++)
            {
                if (Global.CloudStorage.Contains(k) && Global.CloudStorage.IsLocalCell(k))
                {
                    using (var rootCell = Global.LocalStorage.UseSSSPCell(k))
                    {
                        if (rootCell.neighbors.Count == 0) continue;

                        double rankValueP = rootCell.rankValue / rootCell.neighbors.Count;
                        MessageSorter sorter = new MessageSorter(rootCell.neighbors);
                        for (int i = 0; i < Global.SlaveCount; i++)
                        {
                            DistanceUpdatingMessageWriter msg = new DistanceUpdatingMessageWriter(rootCell.CellID.Value, rankValueP, sorter.GetCellRecipientList(i));
                            Global.CloudStorage.DistanceUpdatingToSlave(i, msg);
                        }
                    }
                }
            }
        }

        public override void StartSSSPHandler(StartSSSPMessageReader request)
        {
            //get info
            thresholdFactor = request.thresholdFactor;
            suffix = request.node_count + "_" + thresholdFactor + ".txt";

            //iterations
            writeToFile("Node_count:\t" + request.node_count + "\tThreasholdFactor:\t" + thresholdFactor);
            writeToFile("Iteration\tTime(ms)\tUnConverged_node_count");
            for (int cnt = 0; cnt < request.iteration_time; cnt++)
            {
                DateTime dt = DateTime.Now;
                long node_count = request.node_count;

                step_one(node_count);
                step_two(node_count);

                /*
                using (var rootCell = Global.LocalStorage.UseSSSPCell(0))
                {
                    Console.WriteLine("Cell 0: " + rootCell.rankValue);
                }
                 */
                TimeSpan ts = DateTime.Now - dt;
                
                String s = cnt + "\t" + ts.TotalMilliseconds.ToString() + "\t" + unConvergedNodeCount;
                writeToFile(s);
            }
        }
    }

    class Program
    {
        const double INIRANK = 10000;

        static void printList(List<long> list)
        {
            foreach (long a in list)
            {
                Console.Write(" " + a);
            }
            Console.WriteLine();
        }

        static void loadFile(String path, int node_count)
        {
            for (int i = 0; i < node_count; i++)
            {
                Global.CloudStorage.SaveSSSPCell(i, rankValue: INIRANK, newRankValue: 0, neighbors: new List<long>());
            }

            //path is like: c:\\Thunderbird_Inbox.txt
            StreamReader sr = new StreamReader(path);
            //string working = string.Empty;
            string line = string.Empty;
            HashSet<long> neighbors = new HashSet<long>();

            String key = "";
            long cellId = 0;
            long neighbor = 0;

            while ((line = sr.ReadLine()) != null)
            {
                if (line.StartsWith("#") || (line == ""))
                {
                    continue;
                }

                String[] array = line.Split('\t');
                if ((key != "") && (!array[0].Equals(key)))
                {
                    Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, newRankValue: 0, neighbors: neighbors.ToList());
                    //Console.Write(cellId);printList(neighbors.ToList());
                    
                    neighbors = new HashSet<long>();
                }

                key = array[0];
                cellId = long.Parse(key);
                neighbor = long.Parse(array[1]);

                if (neighbor != cellId) neighbors.Add(neighbor);

                //Console.WriteLine(array[0] + "   " + array[1]);                
            }

            Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, newRankValue: 0, neighbors: neighbors.ToList());
            //Console.Write(cellId);printList(neighbors.ToList());

            //this.Text = "DONE!!";
            sr.Close();
            //Console.ReadLine();
        }

        static void loadFile2(String path, int node_count)
        {
            int specialNode = node_count - 10;
            //status: represents whether a node has outgoing edges or not
            for (int i = 0; i < node_count; i++)
            {
                Global.CloudStorage.SaveSSSPCell(i, neighbors: new List<long>());
            }


            StreamReader sr = new StreamReader(path);
            string line = string.Empty;
            HashSet<long> neighbors = new HashSet<long>();
            HashSet<long> all = new HashSet<long>();

            String key = "";
            long cellId = 0;
            long neighbor = 0;

            while ((line = sr.ReadLine()) != null)
            {
                if (line.StartsWith("#") || (line == ""))
                {
                    continue;
                }

                String[] array = line.Split('\t');
                if ((key != "") && (!array[0].Equals(key)))
                {
                    if (neighbors.Count > 0)
                    {
                        //add special node
                        neighbors.Add(specialNode);
                        all.Add(cellId);
                    }
                    else
                    {
                    }

                    Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, neighbors: neighbors.ToList());

                    neighbors = new HashSet<long>();
                }

                key = array[0];
                cellId = long.Parse(key);
                neighbor = long.Parse(array[1]);

                if (neighbor != cellId) neighbors.Add(neighbor);

            }

            if (neighbors.Count > 0)
            {
                //add special node
                neighbors.Add(specialNode);
            }
            else
            {
            }

            Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, neighbors: neighbors.ToList());

            //save special node
            Global.CloudStorage.SaveSSSPCell(specialNode, rankValue: INIRANK, neighbors: all.ToList());

            sr.Close();
        }

        /*
        static void Main(string[] args)
        {
            //HashSet<long> neighbors = new HashSet<long>();
            //parseFile("C:\\Users\\Administrator\\Desktop\\b.txt");
        }
        */
        
        static void Main(string[] args)
        {
            if (args.Length >= 1 && args[0].StartsWith("-s"))
            {
                SSSPSlave slave = new SSSPSlave();
                slave.Start(true);
            }

            //SSSP.exe -c node_count iteration_time thresholdFactor
            if (args.Length >= 4 && args[0].StartsWith("-c"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;

                long node_count = long.Parse(args[1].Trim());
                long iteration_time = long.Parse(args[2].Trim());
                double threasholdFactor = double.Parse(args[3].Trim());

                DateTime dt = DateTime.Now;
                for (int i = 0; i < Global.SlaveCount; i++)
                {
                    Global.CloudStorage.StartSSSPToSlave(i, new StartSSSPMessageWriter(node_count, iteration_time, threasholdFactor));
                    //Global.CloudStorage.ChangeSSSPToSlave(i, new ChangeSSSPMessageWriter(node_count));
                }
                TimeSpan ts = DateTime.Now - dt;
                Console.WriteLine("All Time --- " + " Node_count: " + node_count + " Time: " + ts.TotalMilliseconds.ToString());
            
            }

            //SSSP.exe -q cellID
            if (args.Length >= 2 && args[0].StartsWith("-q"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                var cell = Global.CloudStorage.LoadSSSPCell(int.Parse(args[1]));
                Console.Write("Current Node's id is {0}, The rankValue is {1}, The neighbours are",
                    cell.CellID, cell.rankValue);

                for (int i = 0; i < cell.neighbors.Count; i++)
                {
                    Console.Write(" " + cell.neighbors[i]);
                }

                Console.WriteLine();
                
            }

            //SSSP.exe -g node_count
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
                    Global.CloudStorage.SaveSSSPCell(i, rankValue: 10000, newRankValue: 0, neighbors: neighbors.ToList());
                }
            }

            //SSSP.exe -r fileName node_count
            //search in the desktop
            if (args.Length >= 3 && args[0].StartsWith("-r"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                String path = "C:\\Users\\Administrator\\Desktop\\" + args[1];
                loadFile(path, int.Parse(args[2]));
            }

            //SSSP.exe -a fileName node_count
            //search in the desktop
            if (args.Length >= 3 && args[0].StartsWith("-a"))
            {
                TrinityConfig.CurrentRunningMode = RunningMode.Client;
                String path = "C:\\Users\\Administrator\\Desktop\\" + args[1];
                loadFile2(path, int.Parse(args[2]));
            }
        }
    }
}
