## How to generate the published application for Azure Batch

This publishes a standalone package:

	dotnet publish -c release -r linux-x64 -o standalone

	tar czf process-data-standalone.tar.gz -C standalone .

	zip process-data-standalone.zip standalone/* -j 

This publishes a smaller package but requires .NET core to be installed on target system.

	dotnet publish -c release -o publish

	tar czf process-data.tar.gz -C publish .
	
	zip process-data.zip publish/* -j 

Look in the start_tasks folder for an example on how to install .NET Core SDK on a pool or job start.

Note #1: Use zip if you want to persistently add the application package through the portal (only accepts zip files) - it works for either Linux or Windows nodes.

Note #2: If using Linux nodes, and adding the file reference yourself when creating the pool, tar is preferable or else you need to run 'sudo apt-get install zip' as a start task on the pool.

## How to make the app available in Azure Batch

**1. Application package**
Just add an application in the portal and upload the zip file. You can then specify the application package at pool or task level (it is reused between tasks on the same node after the first install).

To reference the application in task command lines, Batch service creates environment variables:

Linux:
$AZ_BATCH_APP_PACKAGE_applicationid_version

Windows:
%AZ_BATCH_APP_PACKAGE_APPLICATIONID#version

	Note: Linux flattens periods, hyphens and number signs to underscores, and preserves the case of the application ID.
	For our package, version1 it becomes:

	$AZ_BATCH_APP_PACKAGE_process_data_1

**2. Add as a resource file**
Upload the zip or tar file to an Azure Storage account and add the reference to the pool's Start task or the Job's preparation task. In the task, unpack the file to a known directory.

	/bin/bash -c "cd $AZ_BATCH_NODE_SHARED_DIR && unzip $AZ_BATCH_NODE_STARTUP_DIR/wd/process-data.zip"

