using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BatchProcessor {
    class Program {

        // Batch account credentials
        private const string BatchAccountName = "nunos";
        private const string BatchAccountKey = "WLFUXN0QNLKHK0wlqrPJ11/2fUK6iWhaLS52bQruFn6aEN9LNDzN7XVgp8/QChW+anu2w+vWKOi2glYCkwEKcQ==";
        private const string BatchAccountUrl = "https://nunos.westeurope.batch.azure.com";

        // Storage account credentials
        private const string StorageAccountName = "nunosbatchstorage";
        private const string StorageAccountKey = "xKZevIXNZnSmfVIsVGQ2hMOfMS4D9f+bJGknsUQ+epgQy3tJxv9YEs8RGlS7aGRNIusY1oCEFyStIFpMz+esXg==";

        // Batch resource settings
        private const string PoolIdPrefix = "demopool";
        private const string JobIdPrefix = "demojob";
        private const int PoolNodeCount = 2;
        private const string PoolVMSize = "STANDARD_A1_v2";

        // Azure storage containers
        private const string AppContainerName = "application";
        private const string InputContainerName = "input";
        private const string OutputContainerName = "output";

        // Resource files
        private const string ApplicationFile = "process-data.tar";

        private const int delay = 200;

        /* 
            Steps in the demo:
                - Create Storage Containers
                - upload application file and get access to output container
                - create a pool with a start task that unpacks app into a shared dir
                - create a job
                - create several tasks (one per file to process) that call our application passing the file as a parameter and also an output container for writing the result
                - monitor task execution
                - download results
        */

        static void Main(string[] args) {
            Console.WriteLine("\nBatch Processor");
            Console.WriteLine("----------------------------");
            Execute().GetAwaiter().GetResult();
        }

        static async Task Execute() {

            // setup unique names for pool and job
            var poolId = PoolIdPrefix; // + DateTime.Now.ToString("yyyyMMdd_HHmmss");
            var jobId = JobIdPrefix; // + DateTime.Now.ToString("yyyyMMdd_HHmmss");

            // create a storage blob client to upload files
            var blobClient = CreateBlobClient(StorageAccountName, StorageAccountKey);

            Console.WriteLine("\n>>> Creating Storage Containers <<<\n");

            // Use the blob client to create the containers in Azure Blob Storage
            CreateStorageContainersIfNotExist(blobClient, AppContainerName);
            CreateStorageContainersIfNotExist(blobClient, InputContainerName);
            CreateStorageContainersIfNotExist(blobClient, OutputContainerName);

            PromptContinue();

            Console.WriteLine("\n>>> Setting up storage references <<<\n");

            // get reference to application SAS url
            var application = await UploadFileOrGetReference(blobClient, AppContainerName, ApplicationFile, false);
            Console.WriteLine($"[INFO] {ApplicationFile} SAS: {application.HttpUrl}");

            // get reference to output container SAS uri
            var outputContainerSasUrl = GetContainerSasUrl(blobClient, OutputContainerName);
            Console.WriteLine($"[INFO] {OutputContainerName} SAS: {application.HttpUrl}");

            PromptContinue();

            // up until here, we haven't used the Batch API at all (except for ResourceFile type)
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(BatchAccountUrl, BatchAccountName, BatchAccountKey);

            using(var batchClient = BatchClient.Open(cred)) {
                
                Console.WriteLine("\n>>> Creating Pool <<<\n");
                await CreatePoolIfNotExist(batchClient, poolId, new [] { application });

                PromptContinue();

                Console.WriteLine("\n>>> Creating Job <<<\n");
                await CreateJob(batchClient, jobId, poolId);

                PromptContinue();

                Console.WriteLine("\n>>> Creating Tasks <<<\n");
                await AddAllTasksToJob(jobId, blobClient, outputContainerSasUrl, batchClient);

                Console.WriteLine("\n>>> Monitor tasks <<<\n");
                var tasksSucceeded = await MonitorTasks(batchClient, jobId, TimeSpan.FromMinutes(60));

                if (tasksSucceeded)
                {
                    var outputDir = Directory.CreateDirectory(Path.Combine(Directory.GetCurrentDirectory(),"output"));
                    await DownloadFromContainer(blobClient, OutputContainerName, outputDir.FullName);
                }
 

                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no") {
                    batchClient.JobOperations.DeleteJob(jobId);
                }

                Console.Write("Delete pool? [yes] no: ");
                response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no") {
                    batchClient.PoolOperations.DeletePool(poolId);
                }

            }
        }

        private static void PromptContinue() {
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        private static async Task DownloadFromContainer(CloudBlobClient blobClient, string outputContainerName, string saveIntoDirectory) {
            Console.WriteLine($"Downloading all files from container {outputContainerName}");
            var container = blobClient.GetContainerReference(outputContainerName);
            var items = await container.ListBlobsSegmentedAsync(null, true, new BlobListingDetails(), null, new BlobContinuationToken(), new BlobRequestOptions(), new Microsoft.WindowsAzure.Storage.OperationContext());

            await Task.WhenAll(items.Results.Select(item => {
                var blob = (CloudBlob) item;
                var localOutputFile = Path.Combine(saveIntoDirectory, blob.Name);
                return blob.DownloadToFileAsync(localOutputFile, FileMode.Create);
            }).ToArray());

            Console.WriteLine($"All files downloaded to {saveIntoDirectory}");
        }

        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout) {
            var tasksSuccessful = true;

            var tasks = await batchClient.JobOperations.ListTasks(jobId, new ODATADetailLevel(selectClause: "id")).ToListAsync();
            Console.WriteLine($"Waiting for tasks to complete. Timeout is {timeout}");

            var taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();

            try {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            } catch (TimeoutException) {
                await batchClient.JobOperations.TerminateJobAsync(jobId, "Job timed out");
                Console.WriteLine("Job timed out");
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, "All tasks completed");
            // just because they are completed, doesn't mean they are successful

            foreach (var task in tasks) {
                await task.RefreshAsync(new ODATADetailLevel(selectClause: "id, executionInfo"));
                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure) {
                    tasksSuccessful = false;
                    Console.WriteLine($"Task {task.Id} had a failure: {task.ExecutionInformation.FailureInformation.Message}");
                }
            }

            if (tasksSuccessful) {
                Console.WriteLine("All tasks completed successfully");
            }

            return tasksSuccessful;
        }

        private static async Task AddAllTasksToJob(string jobId, CloudBlobClient blobClient, string outputContainerSasUrl, BatchClient batchClient) {
            var inputFiles = Directory.EnumerateFiles("input_data", "*.dat");

            var tasks = await Task.WhenAll(inputFiles.Select(async inputFile => {
                var uploadedFile = await UploadFileOrGetReference(blobClient, InputContainerName, inputFile, true);
                return CreateTask(jobId, uploadedFile, outputContainerSasUrl);

            }).ToArray());

            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);
        }

        private static CloudTask CreateTask(string jobId, ResourceFile inputFile, string outputContainerSasUrl) {
            var taskId = "simulationTask_" + Path.GetFileNameWithoutExtension(inputFile.FilePath);
            var commandLine = $@"/bin/bash -c ""$AZ_BATCH_NODE_SHARED_DIR/process-data $AZ_BATCH_TASK_WORKING_DIR/{inputFile.FilePath} {delay} \"" { outputContainerSasUrl }\"" { jobId } _ { taskId }""";
            var task = new CloudTask(taskId, commandLine);
            task.ResourceFiles = new[] { inputFile };
            return task;
        }

        private static async Task CreateJob(BatchClient batchClient, string jobId, string poolId) {
            Console.WriteLine($"Creating job { jobId }...");
            var job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            await job.CommitAsync();
        }

        private static async Task CreatePoolIfNotExist(BatchClient batchClient, string poolId, ResourceFile[] resourceFiles) {
            
            var pools = await batchClient.PoolOperations.ListPools().ToListAsync();
            var alreadyExists = pools.Any(x => x.Id == poolId);
            if (alreadyExists) {
                Console.WriteLine($"Pool { poolId } already exists.");
                return;
            }

            Console.WriteLine($"Creating pool { poolId }...");

            var pool = batchClient.PoolOperations.CreatePool(
                poolId: poolId,
                targetLowPriorityComputeNodes: 10,
                virtualMachineSize: "standard_a1_v2 ",
                virtualMachineConfiguration : new VirtualMachineConfiguration(
                    new ImageReference("UbuntuServer ", "Canonical ", "18.04 - LTS"),
                    "batch.node.ubuntu 18.04 "));

            pool.StartTask = new StartTask {
                CommandLine = $@"/bin/bash - c ""cd $AZ_BATCH_NODE_SHARED_DIR && tar -xvf $AZ_BATCH_NODE_STARTUP_DIR/wd/{ApplicationFile}""",
                WaitForSuccess = true,
                ResourceFiles = resourceFiles
        };

        Console.WriteLine($"START TASK: {pool.StartTask.CommandLine}");
        await pool.CommitAsync();
    }

    private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName) {

        var container = blobClient.GetContainerReference(containerName);
        var accessPolicy = new SharedAccessBlobPolicy {
            SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Write
        };

        var accessToken = container.GetSharedAccessSignature(accessPolicy);
        return $"{container.Uri}{accessToken}";

    }

    private static async Task < ResourceFile > UploadFileOrGetReference(CloudBlobClient blobClient, string containerName, string file, bool overwrite) {
        var blobName = Path.GetFileName(file);
        var container = blobClient.GetContainerReference(containerName);
        var blob = container.GetBlockBlobReference(blobName);
        var alreadyExists = await blob.ExistsAsync();

        if (!alreadyExists || overwrite) {
            await blob.UploadFromFileAsync(file);
        }

        var accessPolicy = new SharedAccessBlobPolicy {
            SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
        };

        var accessToken = blob.GetSharedAccessSignature(accessPolicy);
        var blobUri = $"{blob.Uri}{accessToken}";

        return ResourceFile.FromUrl(blobUri, blobName);
    }

    private static void CreateStorageContainersIfNotExist(CloudBlobClient blobClient, string containerName) {

        CloudBlobContainer container = blobClient.GetContainerReference(containerName);

        container.CreateIfNotExistsAsync().Wait();
        Console.WriteLine($"[INFO] Created container {containerName}");
    }

    private static CloudBlobClient CreateBlobClient(string storageAccountName, string storageAccountKey) {
        // Construct the Storage account connection string
        string storageConnectionString =
            $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

        // Retrieve the storage account
        CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

        // Create the blob client
        CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
        return blobClient;
    }

}
}