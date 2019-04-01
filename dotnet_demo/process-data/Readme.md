## How to generate the published application for Azure Batch

This publishes a standalone package (for demo flow #1):

	dotnet publish -c release -r linux-x64 -o standalone

	zip process-data-standalone.zip standalone/* -j 

Note: we use zip because the app will be uploaded as an Application Pacakage through the portal.

This publishes a smaller package but requires .NET core to be installed on target system.

	dotnet publish -c release -o publish

	tar czf process-data.tar.gz -C publish .

Note: we use tar because we'll be deploying this app in Linux nodes and it makes it easier to unpack.

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

or

	/bin/bash -c "cd $AZ_BATCH_NODE_SHARED_DIR && tar -xvf $AZ_BATCH_NODE_STARTUP_DIR/wd/process-data.tar.gz
