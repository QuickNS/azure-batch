using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;

namespace process_data
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Data Processor starting up");
            var filename = args[0];
            Console.WriteLine($"Input file is {filename}");
            var delay = Int32.Parse(args[1]);
            Console.WriteLine($"Estimated Delay is {delay}");
            var outputContainer = args[2];
            Console.WriteLine($"OutputContainerSas is {outputContainer}");
            var outputFile = args[3];
            Console.WriteLine($"OutputFile is {outputFile}");

            Console.WriteLine($"\nCalculating results...\n");
            var i = 0;
            var values = new List<double>();
            using (var reader = File.OpenText(filename))
            {
                while(!reader.EndOfStream)
                {
                    Console.WriteLine($"Processing line {i}");
                    var line = reader.ReadLine();
                    Task.Delay(delay).GetAwaiter().GetResult();
                    var value = line.Split(',').Last();
                    Console.WriteLine($"Result: {value}");
                    i++;
                    values.Add(Double.Parse(value));
                }
            }

            Console.WriteLine($"Uploading results to blob storage...");
            UploadContentToContainerAsync(outputFile, $"Aggregate result: {values.Average()}", outputContainer).GetAwaiter().GetResult();

            Console.WriteLine($"\nData processing finished!");
        }

        private static async Task UploadContentToContainerAsync(string filename, string content, string containerSas)
        {
            try
            {
                CloudBlobContainer container = new CloudBlobContainer(new Uri(containerSas));
                CloudBlockBlob blob = container.GetBlockBlobReference(filename);
                await blob.UploadTextAsync(content);
                Console.WriteLine("Successfully uploaded text for SAS URL " + containerSas);
            }
            catch(StorageException e)
            {
                Console.WriteLine("Failed to upload text for SAS URL " + containerSas);
                Console.WriteLine("Additional error information: " + e.Message);
                Environment.ExitCode = -1;
            }
        }
    }
}
