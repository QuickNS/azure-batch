## Azure Batch demo

This demos illustrates some of the core principles of Azure Batch. It is made up of two different projects:

- **BatchProcessor**: Console application built in .NET Core that uses the Azure Batch SDK to create a pool, a job and a bunch of tasks to do paralell processing of a number of csv files and store the output on Azure Storage.
- **process-data**: this is the actual executable that runs inside each task on Azure Batch. It is also a .NET Core application and can be deployed to either Linux or Windows nodes. It takes a single CSV file as input. processes it and writes the output to a file. This application aims to replicate a legacy application and knows nothing about Azure Storage or batch. It simply reads an input file, processes it and outputs a text file.

## Demo flow

There are actually two distinct flows in this demo, showing off different features of Azure Batch:

### Flow 1

1. In the process-data folder, follow the instructions to build the *standalone* package. This is the console application packed with the .NET framework Core and all dependencies, meant to run in systems without a pre-existing installation of .NET Core.
2. In the Azure portal, in your Azure Batch account, create a new Application and upload the zip file. Make sure the application id is **process-data-standalone** and version is **1**
3. In the BatchProcessor folder, configure settings.json to reference your Azure Batch account and the linked Azure Storage account (it needs to be linked to support the Application Package feature).
4. Execute BatchProcessor and choose demo flow #1 until it pauses. This will create a Linux pool of 10 nodes, referencing the application package you submitted earlier. This will cause Azure Batch to download and unzip the application to every node that gets added into the pool. 
*Note: it will take about 5 minutes to provision the pool*
5. You can manually check this by looking at the nodes in the pool and looking inside the **applications** folder. These folders are accessible through special environment variables set by Azure Batch.
6. [optional] In the portal, create a job named **testjob** and reference the pool we just created. Then add a task and use the following command line:

        /bin/bash -c "$AZ_BATCH_APP_PACKAGE_process_data_standalone_1/process-data"
    Note: the task will fail because we are not supplying the right arguments to the executable, but you should see a message coming from the app itself in *stdout*:
    
        Wrong arguments specified: process-data <filename> <delay> <output_file>

    This means the application is correctly configured and we now reference it in our tasks!

7. Progress with application: this will create a new job and schedule 60 tasks. Each task references a different csv file that gets added as a resource of the task, after being uploaded to Azure Blob Storage. These tasks will take a few minutes to complete.
8. While this happens, you can check the status of tasks in the portal and look at completed tasks to see the output.
9.  Finally, we can check the output of all tasks saved in the Azure Blob Storage.

### Flow 2:

1. In the process-data folder, follow the instructions to build the smaller app package. This is a console application meant to run on systems with a pre-existing installation of .NET Core. This is not available on the Azure Batch nodes, so we'll need to add a Start task to the pool (or a job preparation task) to install .NET core on the system.
2. Copy the tar.gz package to the BatchProcessor folder. Make sure it is named **process-data.tar.gz**.
3. In the BatchProcessor folder, configure settings.json to reference your Azure Batch account and an Azure Storage account (any storage account will do)
4. Execute BatchProcessor and choose demo flow #2. This will execute a number of steps:
   - Uploads the application package to the chosen storage account and get a SAS reference to it
   - Uploads the .NET install script to the storage account
   - Create a pool with a Start task to install .NET Core
   - Create a Job with a job preparation task to download and unpack the application package from the storage account using the SAS token.
   - Schedules 60 tasks to run in the job. Each task references a different csv file that gets added as a resource of the task, after being uploaded to Azure Blob Storage. These tasks will take a few minutes to complete.
8. While this happens, you can check the status of tasks in the portal and look at completed tasks to see the output.
9. Finally, we can check the output of all tasks saved in the Azure Blob Storage account.
