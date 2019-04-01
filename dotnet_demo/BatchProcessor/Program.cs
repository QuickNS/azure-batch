using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using azure_batch_utils;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BatchProcessor
{
    class Program
    {

        // Azure storage containers
        private const string AppContainerName = "application";
        private const string InputContainerName = "input";
        private const string OutputContainerName = "output";
        private const int Delay = 200; //this value controls how long each task runs for. The actual value becomes delay / 10 seconds (e.g delay=200 means around 20 seconds)


        static void Main(string[] args)
        {
            Console.WriteLine("\nBatch Processor");
            Console.WriteLine("----------------------------\n");
            Console.WriteLine("Please choose the desired demo flow:");
            Console.WriteLine("#1 - Using Application Package (default)");
            Console.WriteLine("#2 - Fully automated process");
            var response = Console.ReadLine();
            if (response != "1")
            {
                Execute_2().GetAwaiter().GetResult();
            }
            else
            {
                Execute().GetAwaiter().GetResult();
            }
        }

        static async Task Execute()
        {
            // setup unique names for pool and job
            var poolId = "DemoPool1";
            var jobId = "DemoJob1";
            var application_package_name = "process-data-standalone";
            var application_package_version = "1";

            var settings = Config.LoadAccountSettings();

            // create a storage blob client to upload files
            var blobClient = CreateBlobClient(settings.StorageAccountName, settings.StorageAccountKey);

            Console.WriteLine("\n>>> Creating Storage Containers <<<\n");

            // Use the blob client to create the containers in Azure Blob Storage.
            Console.WriteLine("Creating container to store csv files");
            CreateStorageContainersIfNotExist(blobClient, InputContainerName); // store input csv files
            Console.WriteLine("Creating container to store output files files");
            CreateStorageContainersIfNotExist(blobClient, OutputContainerName); // store output text files

            // connect to Azure Batch account
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(settings.BatchServiceUrl, settings.BatchAccountName, settings.BatchAccountKey);

            using (var batchClient = BatchClient.Open(cred))
            {
                Console.WriteLine("\n>>> Checking Application Packages in the Azure Batch account <<<\n");
                await ListApplicationPackages(batchClient);

                // prepare application pacakage reference
                var appReference = new ApplicationPackageReference
                {
                    ApplicationId = application_package_name,
                    Version = application_package_version
                };

                var app_packages = new List<ApplicationPackageReference> { appReference };

                Console.WriteLine("\n>>> Creating Pool <<<\n");
                await CreatePoolIfNotExist(batchClient, poolId, applicationPackages: app_packages);

                PromptContinue();

                Console.WriteLine("\n>>> Creating Job <<<\n");
                await CreateJob(batchClient, jobId, poolId);

                PromptContinue();

                Console.WriteLine("\n>>> Creating Tasks <<<\n");

                // get reference to output container SAS uri
                Console.WriteLine("Getting reference to container for storing task output");
                var outputContainerSasUrl = GetContainerSasUrl(blobClient, OutputContainerName);
                Console.WriteLine($"[INFO] {OutputContainerName} SAS: {outputContainerSasUrl}");

                Console.WriteLine("\nAdding tasks...");
                var executable = "$AZ_BATCH_APP_PACKAGE_process_data_standalone_1/process-data";
                await AddAllTasksToJob(batchClient, jobId, executable, blobClient, outputContainerSasUrl);

                Console.WriteLine("\n>>> Monitor tasks <<<\n");
                var tasksSucceeded = await MonitorTasks(batchClient, jobId, TimeSpan.FromMinutes(60));

                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.JobOperations.DeleteJob(jobId);
                }

                Console.Write("Delete pool? [yes] no: ");
                response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.PoolOperations.DeletePool(poolId);
                }
            }
        }

        static async Task Execute_2()
        {

            // setup unique names for pool and job
            var poolId = "DemoPool2";
            var jobId = "DemoJob2";
            var ApplicationFile = "process-data.tar.gz";
            var SetupFile = "dotnetcoreinstall.sh";

            var settings = Config.LoadAccountSettings();

            // create a storage blob client to upload files
            var blobClient = CreateBlobClient(settings.StorageAccountName, settings.StorageAccountKey);

            Console.WriteLine("\n>>> Creating Storage Containers <<<\n");

            // Use the blob client to create the containers in Azure Blob Storage
            Console.WriteLine("Creating container to store Application files");
            CreateStorageContainersIfNotExist(blobClient, AppContainerName);
            // Use the blob client to create the containers in Azure Blob Storage.
            Console.WriteLine("Creating container to store csv files");
            CreateStorageContainersIfNotExist(blobClient, InputContainerName); // store input csv files
            Console.WriteLine("Creating container to store output files files");
            CreateStorageContainersIfNotExist(blobClient, OutputContainerName); // store output text files

            PromptContinue();

            Console.WriteLine("\n>>> Setting up storage references <<<\n");

            // get reference to application SAS url
            Console.WriteLine("Uploading .NET Core install script to Azure Storage");
            ResourceFile setupFile = await UploadFileOrGetReference(blobClient, AppContainerName, SetupFile, false);
            Console.WriteLine($"[INFO] {SetupFile} SAS: {setupFile.HttpUrl}");

            // get reference to application SAS url
            Console.WriteLine("Uploading application to Azure Storage");
            ResourceFile application = await UploadFileOrGetReference(blobClient, AppContainerName, ApplicationFile, false);
            Console.WriteLine($"[INFO] {ApplicationFile} SAS: {application.HttpUrl}");

            // get reference to output container SAS uri
            Console.WriteLine("Getting reference to container for storing task output");
            var outputContainerSasUrl = GetContainerSasUrl(blobClient, OutputContainerName);
            Console.WriteLine($"[INFO] {OutputContainerName} SAS: {outputContainerSasUrl}");

            PromptContinue();

            // up until here, we haven't used the Batch API at all (except for ResourceFile type)
            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(settings.BatchServiceUrl, settings.BatchAccountName, settings.BatchAccountKey);

            using (var batchClient = BatchClient.Open(cred))
            {

                Console.WriteLine("\n>>> Creating Pool <<<\n");

                var startTask = new StartTask
                {
                    CommandLine = $@"/bin/bash -c ""sudo sh dotnetcoreinstall.sh""",
                    WaitForSuccess = true,
                    UserIdentity = new UserIdentity(
                        new AutoUserSpecification(
                            scope: AutoUserScope.Pool,
                            elevationLevel: ElevationLevel.Admin)
                    ),
                    ResourceFiles = new[] { setupFile }
                };

                Console.WriteLine("Creating start task:");
                Console.WriteLine($"   CommandLine: {startTask.CommandLine}");
                Console.WriteLine($"   ResourceFiles:");
                foreach (var f in startTask.ResourceFiles)
                    Console.WriteLine($"       {f.HttpUrl}");
                Console.WriteLine();

                await CreatePoolIfNotExist(batchClient, poolId, startTask: startTask);

                PromptContinue();

                Console.WriteLine("\n>>> Creating Job <<<\n");
                JobPreparationTask jobPrepTask = new JobPreparationTask
                {
                    CommandLine = $@"/bin/bash -c ""cd $AZ_BATCH_NODE_SHARED_DIR && tar -xvf $AZ_BATCH_JOB_PREP_WORKING_DIR/{ApplicationFile}""",
                    WaitForSuccess = true,
                    ResourceFiles = new[] { application }
                };

                Console.WriteLine("Creating job preparation task:");
                Console.WriteLine($"   CommandLine: {jobPrepTask.CommandLine}");
                Console.WriteLine($"   ResourceFiles:");
                foreach (var f in jobPrepTask.ResourceFiles)
                    Console.WriteLine($"       {f.HttpUrl}");
                Console.WriteLine();

                await CreateJob(batchClient, jobId, poolId, jobPrepTask: jobPrepTask);

                PromptContinue();

                Console.WriteLine("\n>>> Creating Tasks <<<\n");
                var executable = "dotnet $AZ_BATCH_NODE_SHARED_DIR/process-data.dll";
                await AddAllTasksToJob(batchClient, jobId, executable, blobClient, outputContainerSasUrl);

                Console.WriteLine("\n>>> Monitor tasks <<<\n");
                var tasksSucceeded = await MonitorTasks(batchClient, jobId, TimeSpan.FromMinutes(60));

                // Clean up Batch resources (if the user so chooses)
                Console.WriteLine();
                Console.Write("Delete job? [yes] no: ");
                string response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.JobOperations.DeleteJob(jobId);
                }

                Console.Write("Delete pool? [yes] no: ");
                response = Console.ReadLine().ToLower();
                if (response != "n" && response != "no")
                {
                    batchClient.PoolOperations.DeletePool(poolId);
                }

            }
        }

        private static void PromptContinue()
        {
            Console.WriteLine("\nPress any key to continue...");
            Console.ReadKey();
        }

        private static async Task ListApplicationPackages(BatchClient batchClient)
        {
            // List the applications and their application packages in the Batch account.
            List<ApplicationSummary> applications = await batchClient.ApplicationOperations.ListApplicationSummaries().ToListAsync();
            foreach (ApplicationSummary app in applications)
            {
                Console.WriteLine("ID: {0} | Display Name: {1}", app.Id, app.DisplayName);

                foreach (string version in app.Versions)
                {
                    Console.WriteLine("    Version: {0}", version);
                }
            }

        }

        private static async Task<bool> MonitorTasks(BatchClient batchClient, string jobId, TimeSpan timeout)
        {
            var tasksSuccessful = true;

            var tasks = await batchClient.JobOperations.ListTasks(jobId, new ODATADetailLevel(selectClause: "id")).ToListAsync();
            Console.WriteLine($"Waiting for tasks to complete. Timeout is {timeout}");

            var taskStateMonitor = batchClient.Utilities.CreateTaskStateMonitor();

            try
            {
                await taskStateMonitor.WhenAll(tasks, TaskState.Completed, timeout);
            }
            catch (TimeoutException)
            {
                await batchClient.JobOperations.TerminateJobAsync(jobId, "Job timed out");
                Console.WriteLine("Job timed out");
                return false;
            }

            await batchClient.JobOperations.TerminateJobAsync(jobId, "All tasks completed");
            // just because they are completed, doesn't mean they are successful

            foreach (var task in tasks)
            {
                await task.RefreshAsync(new ODATADetailLevel(selectClause: "id, executionInfo"));
                if (task.ExecutionInformation.Result == TaskExecutionResult.Failure)
                {
                    tasksSuccessful = false;
                    Console.WriteLine($"Task {task.Id} had a failure: {task.ExecutionInformation.FailureInformation.Message}");
                }
            }

            if (tasksSuccessful)
            {
                Console.WriteLine("All tasks completed successfully");
            }

            return tasksSuccessful;
        }

        private static async Task AddAllTasksToJob(BatchClient batchClient, string jobId, string executable, CloudBlobClient blobClient, string outputContainerUrl)
        {
            var inputFiles = Directory.EnumerateFiles("input_data", "*.dat");

            var tasks = await Task.WhenAll(inputFiles.Select(async inputFile =>
            {
                var uploadedFile = await UploadFileOrGetReference(blobClient, InputContainerName, inputFile, true);
                return CreateTask(jobId, executable, uploadedFile, outputContainerUrl);

            }).ToArray());

            await batchClient.JobOperations.AddTaskAsync(jobId, tasks);
        }

        private static CloudTask CreateTask(string jobId, string executable, ResourceFile inputFile, string outputContainerUrl)
        {
            var taskId = "task_" + Path.GetFileNameWithoutExtension(inputFile.FilePath);

            var commandLine = $@"/bin/bash -c ""{executable} $AZ_BATCH_TASK_WORKING_DIR/{inputFile.FilePath} {Delay} output{taskId}.txt""";

            var task = new CloudTask(taskId, commandLine);

            task.ResourceFiles = new[] { inputFile };

            // add output files
            var outputFiles = new List<OutputFile>
                {
                    new OutputFile(
                        filePattern: @"output*.txt",
                        destination: new OutputFileDestination(new OutputFileBlobContainerDestination(
                            containerUrl: outputContainerUrl,
                            path: taskId)),
                            uploadOptions: new OutputFileUploadOptions(
                            uploadCondition: OutputFileUploadCondition.TaskSuccess)),
                };


            task.OutputFiles = outputFiles;

            return task;
        }

        private static async Task CreateJob(BatchClient batchClient, string jobId, string poolId, JobPreparationTask jobPrepTask = null)
        {
            Console.WriteLine($"Creating job { jobId }...");
            var job = batchClient.JobOperations.CreateJob();
            job.Id = jobId;
            job.PoolInformation = new PoolInformation { PoolId = poolId };

            if (jobPrepTask != null)
            {
                job.JobPreparationTask = jobPrepTask;
            }

            await job.CommitAsync();
        }

        private static async Task CreatePoolIfNotExist(BatchClient batchClient, string poolId, StartTask startTask = null, List<ApplicationPackageReference> applicationPackages = null)
        {

            Console.WriteLine($"Creating pool { poolId }...");

            var pool = batchClient.PoolOperations.CreatePool(
                poolId: poolId,
                targetLowPriorityComputeNodes: 10,
                targetDedicatedComputeNodes: 0,
                virtualMachineSize: "standard_a1_v2",
                virtualMachineConfiguration: new VirtualMachineConfiguration(
                    new ImageReference("UbuntuServer", "Canonical", "18.04-LTS"),
                    "batch.node.ubuntu 18.04"));

            if (startTask != null)
            {
                pool.StartTask = startTask;
                Console.WriteLine($"    Start Task: {pool.StartTask.CommandLine}");
            }

            if (applicationPackages != null)
            {
                pool.ApplicationPackageReferences = applicationPackages;
                Console.WriteLine("Application Packages:");
                foreach (var a in applicationPackages)
                {
                    Console.WriteLine($"    {a.ApplicationId}");
                }
            }

            await CreatePoolIfNotExistAsync(batchClient, pool);
        }

        private static async Task<CreatePoolResult> CreatePoolIfNotExistAsync(BatchClient batchClient, CloudPool pool)
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


        private static string GetContainerSasUrl(CloudBlobClient blobClient, string containerName)
        {

            var container = blobClient.GetContainerReference(containerName);
            var accessPolicy = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Write
            };

            var accessToken = container.GetSharedAccessSignature(accessPolicy);
            return $"{container.Uri}{accessToken}";

        }

        private static async Task<ResourceFile> UploadFileOrGetReference(CloudBlobClient blobClient, string containerName, string file, bool overwrite)
        {
            var blobName = Path.GetFileName(file);
            var container = blobClient.GetContainerReference(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            var alreadyExists = await blob.ExistsAsync();

            if (!alreadyExists || overwrite)
            {
                await blob.UploadFromFileAsync(file);
            }

            var accessPolicy = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            var accessToken = blob.GetSharedAccessSignature(accessPolicy);
            var blobUri = $"{blob.Uri}{accessToken}";

            return ResourceFile.FromUrl(blobUri, blobName);
        }

        private static void CreateStorageContainersIfNotExist(CloudBlobClient blobClient, string containerName)
        {

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);

            container.CreateIfNotExistsAsync().Wait();
            Console.WriteLine($"[INFO] Created container {containerName}");
        }

        private static CloudBlobClient CreateBlobClient(string storageAccountName, string storageAccountKey)
        {
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