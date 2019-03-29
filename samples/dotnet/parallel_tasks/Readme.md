## Job Tasks Sample

### Description
This sample shows how to configure job preparation and release tasks.

- It creates a job with a preparation task that creates a file and writes the Node ID.
- It creates several tasks that will run and append their TaskID to the same file.
- It also adds a release task to the job that cleans up the file once the job is over.

### Setup

Fill in the settings in *settings.json* file and rename it to *accountsettings.json*.

The BatchConfig.LoadAccountSettings looks for a file named **accountsettings.json** to retrieve the configuration.