using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Common;
using Microsoft.Extensions.Configuration;

namespace azure_batch_utils
{
    public static class BatchUtils
    {
        public static async Task<CloudPool> CreatePoolIfNotExistAsync(BatchClient batchClient, string poolId, string nodeSize = "standard_a1_v2", int dedicatedNodes = 0, int lowPriorityNodes = 2, int maxTasksPerNode = 1, TaskSchedulingPolicy schedulingPolicy = null, StartTask startTask = null)
        {
            var imageReference = CreateImageReference();
            var vmConfig = CreateVirtualMachineConfiguration(imageReference);

            var pool = batchClient.PoolOperations.CreatePool(
                poolId: poolId,
                targetDedicatedComputeNodes: dedicatedNodes,
                targetLowPriorityComputeNodes: lowPriorityNodes,
                virtualMachineSize: nodeSize,
                virtualMachineConfiguration: vmConfig);

            pool.MaxTasksPerComputeNode = maxTasksPerNode;

            if (schedulingPolicy != null)
            {
                pool.TaskSchedulingPolicy = schedulingPolicy;
            }

            if (startTask != null)
            {
                pool.StartTask = startTask;
            }

            await BatchUtils.CreatePoolIfNotExist(batchClient, pool).ConfigureAwait(continueOnCapturedContext: false);

            return await batchClient.PoolOperations.GetPoolAsync(poolId).ConfigureAwait(continueOnCapturedContext: false);
        }

        private static VirtualMachineConfiguration CreateVirtualMachineConfiguration(ImageReference imageReference)
        {
            return new VirtualMachineConfiguration(
                imageReference: imageReference,
                nodeAgentSkuId: "batch.node.ubuntu 18.04");
        }

        private static ImageReference CreateImageReference()
        {
            return new ImageReference(
                        publisher: "Canonical",
                        offer: "UbuntuServer",
                        sku: "18.04-LTS",
                        version: "latest"
                    );
        }

        private static async Task<CreatePoolResult> CreatePoolIfNotExist(BatchClient batchClient, CloudPool pool)
        {
            bool successfullyCreatedPool = false;

            int targetDedicatedNodeCount = pool.TargetDedicatedComputeNodes ?? 0;
            int targetLowPriorityNodeCount = pool.TargetLowPriorityComputeNodes ?? 0;
            string poolNodeVirtualMachineSize = pool.VirtualMachineSize;
            string poolId = pool.Id;

            // Attempt to create the pool
            try
            {
                // Create an in-memory representation of the Batch pool which we would like to create.  We are free to modify/update 
                // this pool object in memory until we commit it to the service via the CommitAsync method.
                Console.WriteLine("Attempting to create pool: {0}", pool.Id);

                // Create the pool on the Batch Service
                await pool.CommitAsync().ConfigureAwait(continueOnCapturedContext: false);

                successfullyCreatedPool = true;
                Console.WriteLine("Created pool {0} with {1} dedicated and {2} low priority {3} nodes",
                    poolId,
                    targetDedicatedNodeCount,
                    targetLowPriorityNodeCount,
                    poolNodeVirtualMachineSize);
            }
            catch (BatchException e)
            {
                // Swallow the specific error code PoolExists since that is expected if the pool already exists
                if (e.RequestInformation?.BatchError?.Code == BatchErrorCodeStrings.PoolExists)
                {
                    // The pool already existed when we tried to create it
                    successfullyCreatedPool = false;
                    Console.WriteLine("The pool already existed when we tried to create it");
                }
                else
                {
                    throw; // Any other exception is unexpected
                }
            }

            // If the pool already existed, make sure that its targets are correct
            if (!successfullyCreatedPool)
            {
                CloudPool existingPool = await batchClient.PoolOperations.GetPoolAsync(poolId).ConfigureAwait(continueOnCapturedContext: false);

                // If the pool doesn't have the right number of nodes, isn't resizing, and doesn't have
                // automatic scaling enabled, then we need to ask it to resize
                if ((existingPool.CurrentDedicatedComputeNodes != targetDedicatedNodeCount || existingPool.CurrentLowPriorityComputeNodes != targetLowPriorityNodeCount) &&
                    existingPool.AllocationState != AllocationState.Resizing &&
                    existingPool.AutoScaleEnabled == false)
                {
                    // Resize the pool to the desired target. Note that provisioning the nodes in the pool may take some time
                    await existingPool.ResizeAsync(targetDedicatedNodeCount, targetLowPriorityNodeCount).ConfigureAwait(continueOnCapturedContext: false);
                    return CreatePoolResult.ResizedExisting;
                }
                else
                {
                    return CreatePoolResult.PoolExisted;
                }
            }

            return CreatePoolResult.CreatedNew;
        }

        public static async Task<CloudJob> CreateJobIfNotExistAsync(
                            BatchClient batchClient,
                            string poolId,
                            string jobId,
                            bool usesTaskDependencies = false,
                            JobPreparationTask prepTask = null,
                            JobReleaseTask releaseTask = null)
        {
            CloudJob job = await GetJobIfExistAsync(batchClient, jobId).ConfigureAwait(continueOnCapturedContext: false);

            if (job == null)
            {
                Console.WriteLine("Job {0} not found, creating...", jobId);

                CloudJob unboundJob = batchClient.JobOperations.CreateJob(jobId, new PoolInformation() { PoolId = poolId });
                
                unboundJob.UsesTaskDependencies = usesTaskDependencies;

                if (prepTask != null)
                {
                    unboundJob.JobPreparationTask = prepTask;
                }

                if (releaseTask != null)
                {
                    unboundJob.JobReleaseTask = releaseTask;
                }

                await unboundJob.CommitAsync().ConfigureAwait(continueOnCapturedContext: false);
               
                // Get the bound version of the job with all of its properties populated
                job = await batchClient.JobOperations.GetJobAsync(jobId).ConfigureAwait(continueOnCapturedContext: false);
            }

            return job;
        }

        private static async Task<CloudJob> GetJobIfExistAsync(BatchClient batchClient, string jobId)
        {
            Console.WriteLine("Checking for existing job {0}...", jobId);

            // Construct a detail level with a filter clause that specifies the job ID so that only
            // a single CloudJob is returned by the Batch service (if that job exists)
            ODATADetailLevel detail = new ODATADetailLevel(filterClause: string.Format("id eq '{0}'", jobId));
            List<CloudJob> jobs = await batchClient.JobOperations.ListJobs(detailLevel: detail).ToListAsync().ConfigureAwait(continueOnCapturedContext: false);

            return jobs.FirstOrDefault();
        }

        public static async Task WaitForPoolToReachStateAsync(BatchClient client, string poolId, AllocationState targetAllocationState, TimeSpan timeout)
        {
            Console.WriteLine("Waiting for pool {0} to reach allocation state {1}", poolId, targetAllocationState);

            DateTime startTime = DateTime.UtcNow;
            DateTime timeoutAfterThisTimeUtc = startTime.Add(timeout);

            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id,allocationState");
            CloudPool pool = await client.PoolOperations.GetPoolAsync(poolId, detail);

            while (pool.AllocationState != targetAllocationState)
            {
                Console.Write(".");

                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(continueOnCapturedContext: false);
                await pool.RefreshAsync(detail).ConfigureAwait(continueOnCapturedContext: false);

                if (DateTime.UtcNow > timeoutAfterThisTimeUtc)
                {
                    throw new TimeoutException(string.Format("Timed out waiting for pool {0} to reach state {1}", poolId, targetAllocationState));
                }
            }

            Console.WriteLine();
        }

        public static async Task WaitForNodesToReachStateAsync(BatchClient client, string poolId, ComputeNodeState targetNodeState, TimeSpan timeout)
        {
            Console.WriteLine("Waiting for nodes to reach state {0}", targetNodeState);

            DateTime startTime = DateTime.UtcNow;
            DateTime timeoutAfterThisTimeUtc = startTime.Add(timeout);

            CloudPool pool = await client.PoolOperations.GetPoolAsync(poolId).ConfigureAwait(continueOnCapturedContext: false);

            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id,state");
            IEnumerable<ComputeNode> computeNodes = pool.ListComputeNodes(detail);

            while (computeNodes.Any(computeNode => computeNode.State != targetNodeState))
            {
                Console.Write(".");

                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(continueOnCapturedContext: false);
                computeNodes = pool.ListComputeNodes(detail).ToList();

                if (DateTime.UtcNow > timeoutAfterThisTimeUtc)
                {
                    throw new TimeoutException(string.Format("Timed out waiting for compute nodes in pool {0} to reach state {1}", poolId, targetNodeState.ToString()));
                }
            }

            Console.WriteLine();
        }

        public static async Task WaitForJobToReachStateAsync(BatchClient client, string jobId, JobState targetJobState, TimeSpan timeout)
        {
            Console.WriteLine("Waiting for job {0} to reach state {1}", jobId, targetJobState);

            DateTime startTime = DateTime.UtcNow;
            DateTime timeoutAfterThisTimeUtc = startTime.Add(timeout);

            ODATADetailLevel detail = new ODATADetailLevel(selectClause: "id,state");
            CloudJob job = await client.JobOperations.GetJobAsync(jobId, detail);

            while (job.State != targetJobState)
            {
                Console.Write(".");

                await Task.Delay(TimeSpan.FromSeconds(10)).ConfigureAwait(continueOnCapturedContext: false);
                await job.RefreshAsync(detail).ConfigureAwait(continueOnCapturedContext: false);

                if (DateTime.UtcNow > timeoutAfterThisTimeUtc)
                {
                    throw new TimeoutException(string.Format("Timed out waiting for job {0} to reach state {1}", jobId, targetJobState));
                }
            }

            Console.WriteLine();
        }

        public static async Task PrintNodeTasksAsync(BatchClient batchClient, string poolId)
        {
            Console.WriteLine("Listing Node Tasks");
            Console.WriteLine("==================");

            ODATADetailLevel nodeDetail = new ODATADetailLevel(selectClause: "id,recentTasks");
            IPagedEnumerable<ComputeNode> nodes = batchClient.PoolOperations.ListComputeNodes(poolId, nodeDetail);

            await nodes.ForEachAsync(node =>
            {
                Console.WriteLine();
                Console.WriteLine(node.Id + " tasks:");

                if (node.RecentTasks != null && node.RecentTasks.Any())
                {
                    foreach (TaskInformation task in node.RecentTasks)
                    {
                        Console.WriteLine("\t{0}: {1}", task.TaskId, task.TaskState);
                    }
                }
                else
                {
                    // No tasks found for the node
                    Console.WriteLine("\tNone");
                }
            }).ConfigureAwait(continueOnCapturedContext: false);

            Console.WriteLine("==================");
        }

        public static async Task WaitForTasksAndPrintOutputAsync(BatchClient batchClient, IEnumerable<CloudTask> tasks, TimeSpan timeout)
        {
            // We use the task state monitor to monitor the state of our tasks -- in this case we will wait for them all to complete.
            TaskStateMonitor taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();

            // Wait until the tasks are in completed state.
            List<CloudTask> ourTasks = tasks.ToList();

            await taskStateMonitor.WhenAll(ourTasks, TaskState.Completed, timeout).ConfigureAwait(continueOnCapturedContext: false);

            // dump task output
            foreach (CloudTask t in ourTasks)
            {
                Console.WriteLine("Task {0}", t.Id);

                try
                {
                    //Read the standard out of the task
                    NodeFile standardOutFile = await t.GetNodeFileAsync(Constants.StandardOutFileName).ConfigureAwait(continueOnCapturedContext: false);
                    string standardOutText = await standardOutFile.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false);
                    if (standardOutText != String.Empty)
                    {
                        Console.WriteLine("Standard out:");
                        Console.WriteLine(standardOutText);
                    }
                }
                catch (Exception)
                {
                    Console.WriteLine("Could not retrieve Standard Out file");
                }
                try
                {
                    //Read the standard error of the task
                    NodeFile standardErrorFile = await t.GetNodeFileAsync(Constants.StandardErrorFileName).ConfigureAwait(continueOnCapturedContext: false);
                    string standardErrorText = await standardErrorFile.ReadAsStringAsync().ConfigureAwait(continueOnCapturedContext: false);
                    if (standardErrorText != String.Empty)
                        Console.WriteLine("Standard error:");
                    Console.WriteLine(standardErrorText);
                }
                catch (Exception)
                {
                    Console.WriteLine("Could not retrieve Standard Error file");
                }

                Console.WriteLine();
            }
        }

    }
}