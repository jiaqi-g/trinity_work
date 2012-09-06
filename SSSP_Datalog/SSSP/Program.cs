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
        //between 0-1
        private double thresholdFactor = 0;

        private long node_count = 0;

        public String suffix = "undefined";

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
		
        public override void RankUpdatingHandler(RankUpdatingMessageReader request)
        {
            //Console.WriteLine("In rank updating handler");

            request.recipients.ForEach((cellId) =>
                {
                    using (var cell = Global.LocalStorage.UseSSSPCell(cellId))
                    {
                        //Console.WriteLine("Use cell: " + cellId);

                        cell.storedRankValue += request.rankValueP;
                        //Console.WriteLine("Stored: " + cell.storedRankValue);

                        if ((cell.storedRankValue > cell.rankValue * (1 + thresholdFactor)) && (cell.status))
                        {
                            //Console.WriteLine("Stored: " + cell.storedRankValue + " Rank: " + cell.rankValue);
                            //cell.rankValue = cell.storedRankValue;
                            newNodeSet.Add(cellId);
                        }
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
        
        //HashSet<long> localSet = new HashSet<long>();
        HashSet<long> newNodeSet = new HashSet<long>();
        HashSet<long> nodeSet;

        private void performAction(long cellId)
        {
            using (var rootCell = Global.LocalStorage.UseSSSPCell(cellId))
            {
                //Console.WriteLine("PerformAction: " + cellId);
                //update rankValue && storedRankValue
                
                //rootCell.rankValue = rootCell.storedRankValue - rootCell.oldStoredRankValue;
                //rootCell.oldStoredRankValue = rootCell.storedRankValue;
                
                //long rankValueP = (rootCell.rankValue - rootCell.oldRankValue) / rootCell.neighbors.Count;
                double rankValueP = (rootCell.storedRankValue - rootCell.rankValue) / rootCell.neighbors.Count;

                MessageSorter sorter = new MessageSorter(rootCell.neighbors);
                for (int i = 0; i < Global.SlaveCount; i++)
                {
                    //Console.WriteLine(3);
                    RankUpdatingMessageWriter msg = new RankUpdatingMessageWriter(rootCell.CellID.Value, rankValueP, sorter.GetCellRecipientList(i));
                    //Console.WriteLine(4);
                    Global.CloudStorage.RankUpdatingToSlave(i, msg);
                    //Console.WriteLine(5);
                }

                //update
                //Console.WriteLine(6);
                //rootCell.oldRankValue = rootCell.rankValue;
                rootCell.rankValue = rootCell.storedRankValue;
            }
        }

        private void performActionOne(long cellId)
        {
            using (var rootCell = Global.LocalStorage.UseSSSPCell(cellId))
            {




                double rankValueP = rootCell.rankValue / rootCell.neighbors.Count;

                MessageSorter sorter = new MessageSorter(rootCell.neighbors);
                for (int i = 0; i < Global.SlaveCount; i++)
                {
                    //Console.WriteLine(3);
                    RankUpdatingMessageWriter msg = new RankUpdatingMessageWriter(rootCell.CellID.Value, rankValueP, sorter.GetCellRecipientList(i));
                    //Console.WriteLine(4);
                    Global.CloudStorage.RankUpdatingToSlave(i, msg);
                    //Console.WriteLine(5);
                }

                //update
                //Console.WriteLine(6);
                //rootCell.oldRankValue = rootCell.rankValue;
            }
        }

        private void runActiveNodes()
        {
            nodeSet = newNodeSet;
            newNodeSet = new HashSet<long>();

            //iterations
            int cnt = 1;
            while (nodeSet.Count != 0)
            {
                //Console.WriteLine(localSet.Count);
                cnt++;
                DateTime dt = DateTime.Now;
                List<long> list = nodeSet.ToList();
                //Console.WriteLine(-2);
                for (int i = 0; i < list.Count; i++)
                {
                    //Console.WriteLine("-1 " + i);
                    //Console.WriteLine(list[i]);
                    performAction(list[i]);
                    //Console.WriteLine(7);
                    //Console.WriteLine(i);
                }
                TimeSpan ts = DateTime.Now - dt;

                String s = cnt + "\t" + ts.TotalMilliseconds.ToString() + "\t" + list.Count;
                writeToFile(s);

                nodeSet = newNodeSet;
                newNodeSet = new HashSet<long>();
            }
        }

        private void runAllNodes()
        {
            nodeSet = newNodeSet;
            newNodeSet = new HashSet<long>();

            DateTime dt = DateTime.Now;
            List<long> list = nodeSet.ToList();
            
            for (int i = 0; i < list.Count; i++)
            {
                performActionOne(list[i]);
            }
            TimeSpan ts = DateTime.Now - dt;

            String s = "1\t" + ts.TotalMilliseconds.ToString() + "\t" + list.Count;
            writeToFile(s);
        }

        public override void StartSSSPHandler(StartSSSPMessageReader request)
        {
            //get info
            thresholdFactor = request.thresholdFactor;
            node_count = request.node_count;
            suffix = request.node_count + "_" + thresholdFactor + ".txt";

            //
            writeToFile("Node_count:\t" + node_count + "\tThreasholdFactor:\t" + thresholdFactor);
            writeToFile("Iteration\tTime(ms)\tActived_Node_Count");
            
            //set initialization
            DateTime dt = DateTime.Now;
            for (long k = 0; k < request.node_count; k++)
            {
                if (Global.CloudStorage.Contains(k) && Global.CloudStorage.IsLocalCell(k))
                {
                    //!!! UseSSSPCell must be wrapped in using clause
                    using (var rootCell = Global.LocalStorage.UseSSSPCell(k))
                    {
                        if (rootCell.status == true) newNodeSet.Add(k);
                    }
                }
            }
            TimeSpan ts = DateTime.Now - dt;
            Console.WriteLine("Initialization Set Time: " + ts.TotalMilliseconds.ToString());
            //try only active the maximum node //localSet.Add(request.number - 1);
                       
            //doit
            runAllNodes();
            runActiveNodes();

        }
    }

    class Program
    {
        const int INIRANK = 10000;

        static void printList(List<long> list)
        {
            foreach (long a in list)
            {
                Console.Write(" " + a);
            }
            Console.WriteLine();
        }

        /*
         * You have to group the output nodes 
         * 
         */
        static void loadFile(String path, int node_count)
        {
            //status: represents whether a node has outgoing edges or not
            for (int i = 0; i < node_count; i++)
            {
                Global.CloudStorage.SaveSSSPCell(i, neighbors: new List<long>());
            }

            //path is like: c:\\Thunderbird_Inbox.txt
            StreamReader sr = new StreamReader(path);
            //string working = string.Empty;
            string line = string.Empty;
            HashSet<long> neighbors = new HashSet<long>();

            String key = "";
            long cellId = 0;
            long neighbor = 0;
            Boolean status = true;

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
                        status = true;
                    }
                    else
                    {
                        status = false;
                    }

                    Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, status: status, neighbors: neighbors.ToList());
                    //Console.Write(cellId);printList(neighbors.ToList());
                    
                    neighbors = new HashSet<long>();
                }

                key = array[0];
                cellId = long.Parse(key);
                neighbor = long.Parse(array[1]);

                if (neighbor != cellId) neighbors.Add(neighbor);

                //Console.WriteLine(array[0] + "   " + array[1]);                
            }

            if (neighbors.Count > 0)
            {
                status = true;
            }
            else
            {
                status = false;
            }
                
            Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, status: status, neighbors: neighbors.ToList());
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
            Boolean status = true;

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
                        status = true;
                        //add special node
                        neighbors.Add(specialNode);
                        all.Add(cellId);
                    }
                    else
                    {
                        status = false;
                    }

                    Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, status: status, neighbors: neighbors.ToList());
                    
                    neighbors = new HashSet<long>();
                }

                key = array[0];
                cellId = long.Parse(key);
                neighbor = long.Parse(array[1]);

                if (neighbor != cellId) neighbors.Add(neighbor);

            }

            if (neighbors.Count > 0)
            {
                status = true;
                //add special node
                neighbors.Add(specialNode);
            }
            else
            {
                status = false;
            }

            Global.CloudStorage.SaveSSSPCell(cellId, rankValue: INIRANK, status: status, neighbors: neighbors.ToList());
            
            //save special node
            Global.CloudStorage.SaveSSSPCell(specialNode, rankValue: INIRANK, status: status, neighbors: all.ToList());

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
                double thresholdFactor = double.Parse(args[3].Trim());

                DateTime dt = DateTime.Now;
                for (int i = 0; i < Global.SlaveCount; i++)
                {
                    Global.CloudStorage.StartSSSPToSlave(i, new StartSSSPMessageWriter(node_count, thresholdFactor));
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
                Console.Write("Current Node's id is {0}, The RankValue is {1}, The neighbours are",
                    cell.CellID, cell.rankValue);

                for (int i = 0; i < cell.neighbors.Count; i++)
                {
                    Console.Write(" " + cell.neighbors[i]);
                }

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