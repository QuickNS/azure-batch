using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using azure_batch_utils;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.Common;
using Microsoft.Azure.Batch.FileStaging;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace file_handling
{
    class Program
    {
        const string AutoStorageContainerName = "file-handling-demo";
        const string AutoStorageFileName = "input/input_file01.txt";
        const string OutputContainerName = "file-handling-output";
        const string ExternalStorageContainerName = "external-storage-files";

        static void Main(string[] args)
        {
            Console.WriteLine("\nFile Handling Demo");
            Console.WriteLine("----------------------------------\n");
            MainAsync().Wait();
        }

        private static async Task MainAsync()
        {
            const string poolId = "FileHandlingPool";
            const string jobId = "FileHandlingJobDemo";

            var settings = Config.LoadAccountSettings();

            SetupStorage(settings.StorageAccountName, settings.StorageAccountKey);

            BatchSharedKeyCredentials cred = new BatchSharedKeyCredentials(settings.BatchServiceUrl, settings.BatchAccountName, settings.BatchAccountKey);

            using (BatchClient batchClient = BatchClient.Open(cred))
            {
                var pool = await BatchUtils.CreatePoolIfNotExistAsync(batchClient, poolId);

                var job = await BatchUtils.CreateJobIfNotExistAsync(batchClient, poolId, jobId);

                //set up auto storage file
                ResourceFile autoStorageFile = ResourceFile.FromAutoStorageContainer(AutoStorageContainerName, AutoStorageFileName);
                Console.WriteLine("\n[INFO] Autostorage resource File reference: ");
                Console.WriteLine("AutoStorageContainer: " + autoStorageFile.AutoStorageContainerName);
                Console.WriteLine("FilePath: " + autoStorageFile.FilePath);

                //upload file to external storage and add it as a resource file
                string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={settings.StorageAccountName};AccountKey={settings.StorageAccountKey}";

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer externalContainer = blobClient.GetContainerReference(ExternalStorageContainerName);
                await externalContainer.CreateIfNotExistsAsync();
                
                var externalFile = await UploadFileToContainer(blobClient, ExternalStorageContainerName, "resource_files/resource_file.txt", "resource_file.txt");
                Console.WriteLine("\n[INFO] External storage resource File reference:");
                Console.WriteLine("SAS Url: " + externalFile.HttpUrl);
                Console.WriteLine("FilePath: " + externalFile.FilePath);


                // using staging files API
                var filesToStage = new List<IFileStagingProvider>();
                StagingStorageAccount fileStagingStorageAccount = new StagingStorageAccount(
                    storageAccount: settings.StorageAccountName,
                    storageAccountKey: settings.StorageAccountKey,
                    blobEndpoint: storageAccount.BlobEndpoint.ToString());

                FileToStage stagedFile = new FileToStage("resource_files/staged_file.txt", fileStagingStorageAccount);
                Console.WriteLine("\n[INFO] Staged File added:");
                Console.WriteLine("Local File: " + stagedFile.LocalFileToStage);
                Console.WriteLine("Node File: " + stagedFile.NodeFileName);

                filesToStage.Add(stagedFile);

                // setup output files
                // Generate SAS for outputcontainer
                CloudBlobContainer outputContainer = blobClient.GetContainerReference(OutputContainerName);
                await outputContainer.CreateIfNotExistsAsync();

                string containerSas = outputContainer.GetSharedAccessSignature(new SharedAccessBlobPolicy()
                {
                    Permissions = SharedAccessBlobPermissions.Write,
                    SharedAccessExpiryTime = DateTimeOffset.UtcNow.AddDays(1)
                });
                string containerUrl = outputContainer.Uri.AbsoluteUri + containerSas;
                Console.WriteLine("\n[INFO] Output container: " + containerUrl);

                Console.WriteLine("\nPress return to continue...");
                Console.ReadLine();

                // Create tasks
                List<CloudTask> tasks = new List<CloudTask>();

                for (var i = 1; i <= 10; i++)
                {
                    var taskId = i.ToString().PadLeft(3, '0');
                    var commandLine = $@"/bin/bash -c ""echo 'Hello from {taskId}' && printf 'root dir:\n' > output.txt && ls -la >> output.txt && printf '\ninput dir:\n' >> output.txt && ls -la input >> output.txt""";
                    var task = new CloudTask(taskId, commandLine);

                    // add resource files to task (one autostorage, one in external storage)
                    task.ResourceFiles = new[] { autoStorageFile, externalFile };

                    // add staged files
                    task.FilesToStage = filesToStage;

                    // add output files
                    var outputFiles = new List<OutputFile>
                        {
                            new OutputFile(
                                filePattern: @"../std*.txt",
                                destination: new OutputFileDestination(new OutputFileBlobContainerDestination(
                                    containerUrl: containerUrl,
                                    path: taskId)),
                                uploadOptions: new OutputFileUploadOptions(
                                    uploadCondition: OutputFileUploadCondition.TaskCompletion)),
                            new OutputFile(
                                filePattern: @"output.txt",
                                destination: new OutputFileDestination(new OutputFileBlobContainerDestination(
                                    containerUrl: containerUrl,
                                    path: taskId + @"\output.txt")),
                                uploadOptions: new OutputFileUploadOptions(
                                    uploadCondition: OutputFileUploadCondition.TaskCompletion)),
                        };

                    task.OutputFiles = outputFiles;

                    tasks.Add(task);
                }
                
                Console.WriteLine("Submitting tasks and awaiting completion...");
                // Add all tasks to the job.
                batchClient.JobOperations.AddTask(job.Id, tasks);

                await BatchUtils.WaitForTasksAndPrintOutputAsync(batchClient, job.ListTasks(), TimeSpan.FromMinutes(30));
                
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

        private static async void SetupStorage(string storageAccountName, string storageAccountKey)
        {
            string storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName};AccountKey={storageAccountKey}";

            // Retrieve the storage account
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);

            // Create the blob client
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();

            // upload to AutoStorage
            CloudBlobContainer container = blobClient.GetContainerReference(AutoStorageContainerName);
            container.CreateIfNotExistsAsync().Wait();
            await UploadFileToContainer(blobClient, AutoStorageContainerName, "resource_files/input_file01.txt", AutoStorageFileName);
        }

        private static async Task<ResourceFile> UploadFileToContainer(CloudBlobClient blobClient, string containerName, string localFilePath, string destFilePath)
        {
            Console.WriteLine("Uploading file {0} to container [{1}]...", localFilePath, containerName);

            localFilePath = Path.Combine(Environment.CurrentDirectory, localFilePath);

            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            CloudBlockBlob blobData = container.GetBlockBlobReference(destFilePath);
            await blobData.UploadFromFileAsync(localFilePath);

            // Set the expiry time and permissions for the blob shared access signature. In this case, no start time is specified,
            // so the shared access signature becomes valid immediately
            SharedAccessBlobPolicy sasConstraints = new SharedAccessBlobPolicy
            {
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(2),
                Permissions = SharedAccessBlobPermissions.Read
            };

            // Construct the SAS URL for blob
            string sasBlobToken = blobData.GetSharedAccessSignature(sasConstraints);
            string blobSasUri = String.Format("{0}{1}", blobData.Uri, sasBlobToken);

            return ResourceFile.FromUrl(blobSasUri, Path.GetFileName(destFilePath));
        }
    }
}