## How to generate the published application for Azure Batch

This publishes a standalone package:

	dotnet publish -c release -r linux-x64

	tar -cf process-data.tar ./bin/release/netcoreapp2.2/linux-x64/publish/*

This publishes a smaller package but requires .NET core to be installed on target system. Look in the start_tasks folder for an example on how to install .NET Core SDK on a pool start task.

	dotnet publish -c release

	tar -cf process-data.tar ./bin/release/netcoreapp2.2/linux-x64/publish/*


