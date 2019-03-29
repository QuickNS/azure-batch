using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using azure_batch_utils;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;


namespace task_dependencies
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("\nTasks Dependencies Demo");
            Console.WriteLine("----------------------------------\n");
            MainAsync().Wait();
        }

        private static async Task MainAsync()
        {
            const int nodeCount = 1;

            string poolId = "TaskDependenciesSamplePool";
            string jobId = "TaskDependenciesJob";

            var settings = Config.LoadAccountSettings();

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(settings.BatchServiceUrl, settings.BatchAccountName, settings.BatchAccountKey);

            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                var pool = await BatchUtils.CreatePoolIfNotExistAsync(batchClient, poolId, lowPriorityNodes: nodeCount);

                var job = await BatchUtils.CreateJobIfNotExistAsync(batchClient, pool.Id, jobId, usesTaskDependencies: true);

                string taskOutputFile = "$AZ_BATCH_NODE_SHARED_DIR/task_output.txt";

                // Create the collection of tasks that will be added to the job.
                List<CloudTask> tasks = new List<CloudTask>
                    {
                        // 'Rain' and 'Sun' don't depend on any other tasks
                        new CloudTask("Rain", $"/bin/bash -c \"echo Rain >> {taskOutputFile}\""),
                        new CloudTask("Sun", $"/bin/bash -c \"echo Sun >> {taskOutputFile}\""),
 
                        // Task 'Flowers' depends on completion of both 'Rain' and 'Sun'
                        // before it is run.
                        new CloudTask("Flowers", $"/bin/bash -c \"echo Flowers >> {taskOutputFile}\"")
                        {
                            DependsOn = TaskDependencies.OnIds("Rain", "Sun")
                        },
 
                        // Tasks 1, 2, and 3 don't depend on any other tasks. Because
                        // we will be using them for a task range dependency, we must
                        // specify string representations of integers as their ids.
                        new CloudTask("1", $"/bin/bash -c \"echo 1  >> {taskOutputFile}\""),
                        new CloudTask("2", $"/bin/bash -c \"echo 2  >> {taskOutputFile}\""),
                        new CloudTask("3", $"/bin/bash -c \"echo 3  >> {taskOutputFile}\""),

                        // Task dependency on ID range
                        new CloudTask("Final", $"/bin/bash -c \"echo Final >> {taskOutputFile}\""){
                            DependsOn = TaskDependencies.OnIdRange(1,3)
                        },

                        // Task A is the parent task.
                        new CloudTask("A", $"/bin/bash -c \"echo A  >> {taskOutputFile}\"")
                        {
                            // Specify exit conditions for task A and their dependency actions.
                            ExitConditions = new ExitConditions
                            {
                                // If task A exits with a pre-processing error, block any downstream tasks (in this example, task B).
                                PreProcessingError = new ExitOptions
                                {
                                    DependencyAction = DependencyAction.Block
                                },
                                // If task A exits with the specified error codes, block any downstream tasks (in this example, task B).
                                ExitCodes = new List<ExitCodeMapping>
                                {
                                    new ExitCodeMapping(10, new ExitOptions() { DependencyAction = DependencyAction.Block }),
                                    new ExitCodeMapping(20, new ExitOptions() { DependencyAction = DependencyAction.Block })
                                },
                                // If task A succeeds or fails with any other error, any downstream tasks become eligible to run 
                                // (in this example, task B).
                                Default = new ExitOptions
                                {
                                    DependencyAction = DependencyAction.Satisfy
                                },

                            }
                        },
                        // Task B depends on task A. Whether it becomes eligible to run depends on how task A exits.
                        new CloudTask("B", $"/bin/bash -c \"echo B  >> {taskOutputFile}\"")
                        {
                            DependsOn = TaskDependencies.OnId("A")
                        },
                    };
                // Add the tasks in one API call as opposed to a separate AddTask call for each. Bulk task
                // submission helps to ensure efficient underlying API calls to the Batch service.
                Console.WriteLine("Submitting tasks and awaiting completion...");
                await batchClient.JobOperations.AddTaskAsync(job.Id, tasks);

                // Wait for the tasks to complete before proceeding. The long timeout here is to allow time
                // for the nodes within the pool to be created and started if the pool had not yet been created.
                await batchClient.Utilities.CreateTaskStateMonitor().WhenAll(
                    job.ListTasks(),
                    TaskState.Completed,
                    TimeSpan.FromMinutes(30));

                Console.WriteLine("All tasks completed.");
                Console.WriteLine();

                // Print the contents of the shared text file modified by the job preparation and other tasks.
                ODATADetailLevel nodeDetail = new ODATADetailLevel(selectClause: "id, state");
                IPagedEnumerable<ComputeNode> nodes = batchClient.PoolOperations.ListComputeNodes(poolId, nodeDetail);
                await nodes.ForEachAsync(async (node) =>
                {
                    // Check to ensure that the node is Idle before attempting to pull the text file.
                    // If the pool was just created, there is a chance that another node completed all
                    // of the tasks prior to the other node(s) completing their startup procedure.
                    if (node.State == ComputeNodeState.Idle)
                    {
                        NodeFile sharedTextFile = await node.GetNodeFileAsync("shared/task_output.txt");
                        Console.WriteLine("Contents of {0} on {1}:", sharedTextFile.Path, node.Id);
                        Console.WriteLine("-------------------------------------------");
                        Console.WriteLine(await sharedTextFile.ReadAsStringAsync());
                    }
                });

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