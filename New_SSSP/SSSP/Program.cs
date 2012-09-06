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

    class SSSPSlave : SSSPSlaveBase
    {
        public static TextWriter writer = File.CreateText("log");
        //between 0-1
        private long node_count = 0;
        public String suffix = "undefined";
        public double precision = 0;

        public const double baseline = 1.0;
        public static int i = 0;
        public static int v = 0;

        public static long numOfMessage = 0;
        public static double level = 0.001;
        public static int interval = 300;
		
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

        public static bool isAllReceived(List<bool> activeList, List<bool> receivedList)
        {
            bool result = true;
            bool done = false;
            
            for (int i = 0; i < activeList.Count; i++)
            {
                if (activeList[i] == true) {
                    done = true;
                    if (receivedList[i] == false)
                    {
                        result = false;
                        break;
                    }
                }
            }
            if (done) return result;
            else return false;
        }

        public override void RankUpdatingHandler(RankUpdatingMessageReader request)
        {
            //v++;
            //if (v >= 30000)
            //{
              //  return;
            //}

            //Console.WriteLine("In rank updating handler");

            long senderId = request.senderId;
            double value = request.value;
            
            request.recipients.ForEach((cellId) =>
            {
                using (var cell = Global.LocalStorage.UseSSSPCell(cellId))
                {
                    bool toPropagate = false;
                    bool isActive = false;
                    bool recompute = false;
                    bool increasement = false;

					int index = binarySearch(cell.inNeighbors, request.senderId);

                    if (value > cell.inNeighborsValue[index])
                    {
                        cell.inNeighborsValue[index] = value;
                        increasement = true;
                    }

                    /*
                    if (cellId == 2)
                    {
                        v++;
                        if (v % 10000 == 0) Console.WriteLine(cellId + "-Start: Sum " + cell.sum);
                        //" Received Message: (" + request.isActive + "," + request.value + ") " +
                        //" stored_inneighbor_ActiveStatus: " + cell.inNeighborsActiveStatus[index]);
                    }*/

                    if (!request.isActive)
                    {
                        if (cell.inNeighborsActiveStatus[index])
                        {
                            cell.inNeighborsActiveStatus[index] = false;
                            cell.inNeighborsReceivedStatus[index] = false;
                            //clear
                            /*
                            for (int j = 0; j < cell.inNeighborsReceivedStatus.Count; j++)
                            {
                                cell.inNeighborsReceivedStatus[j] = false;
                            }
                            cell.receivedNeighborCount = 0;
                            */
                        }
                        else
                        {
                            //Console.WriteLine("Impossible! " + " receive message (" + request.isActive + "," + request.value + ") from " + request.senderId);
                        }
                    }
                    else
                    {
                        recompute = true;

                        if (cell.inNeighborsActiveStatus[index])
                        {
                            //cell.inNeighborsValue[index] = Math.Max(cell.inNeighborsValue[index], value);
                            //active
                            cell.inNeighborsReceivedStatus[index] = true;
                        }
                        else
                        {
                            //set active
                            if (increasement)
                            {
                                //cell.inNeighborsValue[index] = value;
                                cell.inNeighborsActiveStatus[index] = true;
                                cell.inNeighborsReceivedStatus[index] = true;
                            }
                        }
                    }

                    if (recompute)
                    {
                        //cell.activeNeighborCount != 0
                        //recompute
                        double sum = 0;
                        for (int j = 0; j < cell.inNeighbors.Count; j++)
                        {
                            sum += cell.inNeighborsValue[j];
                        }

                        if (sum > cell.sum)
                        {
                            cell.sum = sum;
                            isActive = true;
                            //if (sum > 1) isActive = true;
                            //else isActive = false;
                        }
                        else
                        {
                            isActive = false;
                        }
                    }

                    //all received
                    if (isAllReceived(cell.inNeighborsActiveStatus, cell.inNeighborsReceivedStatus))
                    {
                        toPropagate = true;
                        //if (cellId == 1) Console.WriteLine(sum);
                    }

                    Console.WriteLine(cellId + " receive message (" + request.isActive + "," + request.value + ") from " + request.senderId);

                    /*
                    if (cellId == 0)
                    {
                        Console.WriteLine(cellId + "-End: Sum " + cell.sum +
                            " Received Message: (" + request.isActive + "," + request.value + ")" +
                            " isPropagate: " + toPropagate);
                    }*/

                    if (toPropagate)
                    {
                        propagate(cell, isActive);
                    }
                }
            });

            //Console.WriteLine("---zz---" + request.senderId);
        }

        private void propagate(SSSPCell cell, bool isActive)
        {
            //clear
            for (int i = 0; i < cell.inNeighborsReceivedStatus.Count; i++)
            {
                cell.inNeighborsReceivedStatus[i] = false;
            }
            //cell.receivedNeighborCount = 0;

            //send-out
            double v = cell.sum / cell.outNeighbors.Count;

            MessageSorter sorter = new MessageSorter(cell.outNeighbors);
            for (int i = 0; i < Global.SlaveCount; i++)
            {
                //Console.WriteLine(sorter.GetCellRecipientList(i).Count);
                RankUpdatingMessageWriter msg = new RankUpdatingMessageWriter(cell.CellID, isActive, v, sorter.GetCellRecipientList(i));
                Global.CloudStorage.RankUpdatingToSlave(i, msg);
            }
        }

        private void initialPropagate(SSSPCell cell, bool isActive)
        {
            //clear
            for (int i = 0; i < cell.inNeighborsReceivedStatus.Count; i++)
            {
                cell.inNeighborsReceivedStatus[i] = false;
            }
            //cell.receivedNeighborCount = 0;

            //send-out
            double v = 1.0 / cell.outNeighbors.Count;

            MessageSorter sorter = new MessageSorter(cell.outNeighbors);
            for (int i = 0; i < Global.SlaveCount; i++)
            {
                //Console.WriteLine(sorter.GetCellRecipientList(i).Count);
                RankUpdatingMessageWriter msg = new RankUpdatingMessageWriter(cell.CellID, isActive, v, sorter.GetCellRecipientList(i));
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
            //writeToFile("Node_count:\t" + node_count + "\tPrecision:\t" + precision);
            //writeToFile("Iteration\tTime(ms)\tActived_Node_Count");

            //doit
            //parseFile("C:\\Users\\Administrator\\Desktop\\b.txt");
            for (long k = 0; k < request.node_count; k++)
            {
                if (Global.CloudStorage.Contains(k) && Global.CloudStorage.IsLocalCell(k))
                {
                    //!!! UseSSSPCell must be wrapped in using clause
                    using (var cell = Global.LocalStorage.UseSSSPCell(k))
                    {
                        initialPropagate(cell, true);
                    }
                }
            }

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
                String line = null; // sr.ReadLine();

                while ((line = sr.ReadLine()) != null)
                {
                    String[] array = line.Split('\t');

                    int cellId = int.Parse(array[0]);
                    int neighborId = int.Parse(array[1]);

                    nodes[cellId].addOutNeighbor(nodes[neighborId]);
                    nodes[neighborId].addInNeighbor(nodes[cellId]);

                    //Console.WriteLine(cellId + " " + neighborId);
                }
            }

            foreach (Node node in nodes)
            {
				node.sortNeighbors();
			
                int inLength = node.inNeighbors.Count;
                List<double> inNeighborsValue = new List<double>(inLength);
                List<bool> inNeighborsActiveStatus = new List<bool>(inLength);
                List<bool> inNeighborsReceivedStatus = new List<bool>(inLength);

                for (int i = 0; i < inLength; i++)
                {
                    inNeighborsValue.Add(0);
                    inNeighborsActiveStatus.Add(true);
                    inNeighborsReceivedStatus.Add(false);
                }

                //Console.WriteLine("Ini inLength: " + node.node_id + " " + inLength);

                Global.CloudStorage.SaveSSSPCell(node.node_id,
                inNeighbors: node.inNeighbors, outNeighbors: node.outNeighbors, /*activeNeighborCount: inLength,*/
                inNeighborsValue: inNeighborsValue, inNeighborsActiveStatus: inNeighborsActiveStatus, inNeighborsReceivedStatus: inNeighborsReceivedStatus);
            }
        }

        static void print(SSSPCell cell)
        {
            Console.WriteLine("Id: " + cell.CellID);
            Console.WriteLine("Sum: " + cell.sum);

            //Console.WriteLine("outNeighbors: " + printList(cell.outNeighbors));
            //Console.WriteLine("Active Neighbor Count: " + cell.activeNeighborCount);
            //Console.WriteLine("Received Neighbor Count: " + cell.receivedNeighborCount);


            Console.WriteLine("inNeighbors: " + printList(cell.inNeighbors));
            Console.WriteLine("inNeighborsActiveStatus: " + printList(cell.inNeighborsActiveStatus));
            Console.WriteLine("inNeighborsReceivedStatus: " + printList(cell.inNeighborsReceivedStatus));
            Console.WriteLine("inNeighborsValue: " + printList(cell.inNeighborsValue));
        }
               
        static String printList(List<long> list)
        {
            String s = "";
            for (int i = 0; i < list.Count; i++)
            {
                s += list[i] + ",";
            }
            return s;
        }

        static String printList(List<bool> list)
        {
            String s = "";
            for (int i = 0; i < list.Count; i++)
            {
                s += list[i] + ",";
            }
            return s;
        }

        static String printList(List<double> list)
        {
            String s = "";
            for (int i = 0; i < list.Count; i++)
            {
                s += list[i] + ",";
            }
            return s;
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
                print(cell);

                Console.WriteLine();
            }
			
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