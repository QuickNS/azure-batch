using System.IO;
using Microsoft.Extensions.Configuration;

namespace azure_batch_utils
{
    public static class Config
    {
        public static AccountSettings LoadAccountSettings()
        {
            if (File.Exists("accountsettings.json"))
            {
                AccountSettings accountSettings = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("accountsettings.json")
                    .Build()
                    .Get<AccountSettings>();
                return accountSettings;
            }
            else
            {
                AccountSettings accountSettings = new ConfigurationBuilder()
                    .SetBasePath(Directory.GetCurrentDirectory())
                    .AddJsonFile("settings.json")
                    .Build()
                    .Get<AccountSettings>();
                return accountSettings;
            }
        }

    }
}