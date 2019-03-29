using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using System.Diagnostics;
using azure_batch_utils;

namespace job_tasks
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("\nParallel Tasks Demo");
            Console.WriteLine("----------------------------------\n");
            MainAsync().Wait();
        }

        private static async Task MainAsync()
        {
            //const string nodeSize = "standard_a1_v2";
            const string nodeSize = "standard_a4_v2";
            const int nodeCount = 4;
            const int maxTasksPerNode = 4; //can be set to 4* the number of cores on each node
            const int taskCount = 32;

            // Ensure there are enough tasks to help avoid hitting some timeout conditions below
            int minimumTaskCount = nodeCount * maxTasksPerNode * 2;
            if (taskCount < minimumTaskCount)
            {
                Console.WriteLine("You must specify at least two tasks per node core for this sample ({0} tasks in this configuration).", minimumTaskCount);
                Console.WriteLine();

                // Not enough tasks, exit the application
                return;
            }

            // In this sample, the tasks simply ping localhost on the compute nodes; adjust these
            // values to simulate variable task duration
            const int minPings = 30;
            const int maxPings = 60;

            const string poolId = "ParallelTasksPoolDemo";
            const string jobId = "ParallelTasksJob";

            var settings = Config.LoadAccountSettings();

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(settings.BatchServiceUrl, settings.BatchAccountName, settings.BatchAccountKey);

            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                var pool = await BatchUtils.CreatePoolIfNotExistAsync(
                    batchClient,
                    poolId,
                    nodeSize: nodeSize,
                    maxTasksPerNode: maxTasksPerNode,
                    schedulingPolicy: new TaskSchedulingPolicy(ComputeNodeFillType.Pack),
                    lowPriorityNodes: nodeCount);

                CloudJob job = await BatchUtils.CreateJobIfNotExistAsync(batchClient, poolId, jobId);

                // The job's tasks ping localhost a random number of times between minPings and maxPings.
                // Adjust the minPings/maxPings values above to experiment with different task durations.
                Random rand = new Random();
                List<CloudTask> tasks = new List<CloudTask>();
                for (int i = 1; i <= taskCount; i++)
                {
                    string taskId = "task" + i.ToString().PadLeft(3, '0');
                    string taskCommandLine = "ping -c " + rand.Next(minPings, maxPings + 1).ToString() + " localhost";
                    CloudTask task = new CloudTask(taskId, taskCommandLine);
                    tasks.Add(task);
                }

                // Pause execution until the pool is steady and its compute nodes are ready to accept jobs.
                // NOTE: Such a pause is not necessary within your own code. Tasks can be added to a job at any point and will be 
                // scheduled to execute on a compute node as soon any node has reached Idle state. Because the focus of this sample 
                // is the demonstration of running tasks in parallel on multiple compute nodes, we wait for all compute nodes to 
                // complete initialization and reach the Idle state in order to maximize the number of compute nodes available for 
                // parallelization.
                await BatchUtils.WaitForPoolToReachStateAsync(batchClient, poolId, AllocationState.Steady, TimeSpan.FromMinutes(30));
                await BatchUtils.WaitForNodesToReachStateAsync(batchClient, poolId, ComputeNodeState.Idle, TimeSpan.FromMinutes(30));


                // Add the tasks in one API call as opposed to a separate AddTask call for each. Bulk task
                // submission helps to ensure efficient underlying API calls to the Batch service.
                Console.WriteLine("Submitting tasks and awaiting completion...");
                await batchClient.JobOperations.AddTaskAsync(job.Id, tasks);

                // Pause again to wait until *all* nodes are running tasks
                await BatchUtils.WaitForNodesToReachStateAsync(batchClient, poolId, ComputeNodeState.Running, TimeSpan.FromMinutes(2));

                Stopwatch stopwatch = Stopwatch.StartNew();

                // Print out task assignment information.
                Console.WriteLine();
                await BatchUtils.PrintNodeTasksAsync(batchClient, poolId);
                Console.WriteLine();

                // Pause execution while we wait for all of the tasks to complete
                Console.WriteLine("Waiting for task completion...");
                Console.WriteLine();

                try
                {
                    await batchClient.Utilities.CreateTaskStateMonitor().WhenAll(
                        job.ListTasks(),
                        TaskState.Completed,
                        TimeSpan.FromMinutes(30));
                }
                catch (TimeoutException e)
                {
                    Console.WriteLine(e.ToString());
                }

                stopwatch.Stop();

                // Obtain the tasks, specifying a detail level to limit the number of properties returned for each task.
                // If you have a large number of tasks, specifying a DetailLevel is extremely important in reducing the
                // amount of data transferred, lowering your query response times in increasing performance.
                ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id,commandLine,nodeInfo,state");
                IPagedEnumerable<CloudTask> allTasks = batchClient.JobOperations.ListTasks(job.Id, detail);

                // Get a collection of the completed tasks sorted by the compute nodes on which they executed
                List<CloudTask> completedTasks = allTasks
                    .Where(t => t.State == TaskState.Completed)
                    .OrderBy(t => t.ComputeNodeInformation.ComputeNodeId)
                    .ToList();

                // Print the completed task information
                Console.WriteLine();
                Console.WriteLine("Completed tasks:");
                string lastNodeId = string.Empty;
                foreach (CloudTask task in completedTasks)
                {
                    if (!string.Equals(lastNodeId, task.ComputeNodeInformation.ComputeNodeId))
                    {
                        Console.WriteLine();
                        Console.WriteLine(task.ComputeNodeInformation.ComputeNodeId);
                    }

                    lastNodeId = task.ComputeNodeInformation.ComputeNodeId;

                    Console.WriteLine("\t{0}: {1}", task.Id, task.CommandLine);
                }

                // Get a collection of the uncompleted tasks which may exist if the TaskMonitor timeout was hit
                List<CloudTask> uncompletedTasks = allTasks
                    .Where(t => t.State != TaskState.Completed)
                    .OrderBy(t => t.Id)
                    .ToList();

                // Print a list of uncompleted tasks, if any
                Console.WriteLine();
                Console.WriteLine("Uncompleted tasks:");
                Console.WriteLine();
                if (uncompletedTasks.Any())
                {
                    foreach (CloudTask task in uncompletedTasks)
                    {
                        Console.WriteLine("\t{0}: {1}", task.Id, task.CommandLine);
                    }
                }
                else
                {
                    Console.WriteLine("\t<none>");
                }

                // Print some summary information
                Console.WriteLine();
                Console.WriteLine("             Nodes: " + nodeCount);
                Console.WriteLine("         Node size: " + nodeSize);
                Console.WriteLine("Max tasks per node: " + pool.MaxTasksPerComputeNode);
                Console.WriteLine("             Tasks: " + tasks.Count);
                Console.WriteLine("          Duration: " + stopwatch.Elapsed);
                Console.WriteLine();
                Console.WriteLine("Done!");
                Console.WriteLine();

                // Clean up the resources we've created in the Batch account
                Console.WriteLine();
                Console.WriteLine("Delete job? [yes] no");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    // Note that deleting the job will execute the job release task if the job was not previously terminated
                    await batchClient.JobOperations.DeleteJobAsync(job.Id);
                }

                Console.WriteLine("Delete pool? [yes] no");
                response = Console.ReadLine();
                if (response != "n" && response != "no")
                {
                    await batchClient.PoolOperations.DeletePoolAsync(poolId);
                }

            }
        }
 



    }
}