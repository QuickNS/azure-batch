using System.IO;
using Microsoft.Extensions.Configuration;

namespace azure_batch_utils
{
    public static class Config
    {
         public static AccountSettings LoadAccountSettings()
        {
            AccountSettings accountSettings = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("accountsettings.json")
                .Build()
                .Get<AccountSettings>();
            return accountSettings;
        }

    }
}