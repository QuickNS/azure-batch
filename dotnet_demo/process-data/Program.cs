using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;

namespace process_data
{
    class Program
    {
        static void Main(string[] args)
        {
            if (args.Length < 3)
            {
                Console.WriteLine("Wrong arguments specified: process-data <filename> <delay> <output_file>");
                Environment.Exit(-1);
            }

            Console.WriteLine("Data Processor starting up");
            var filename = args[0];
            Console.WriteLine($"Input file is {filename}");
            var delay = Int32.Parse(args[1]);
            Console.WriteLine($"Estimated Delay is {delay}");
            var outputFile = args[2];
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

            Console.WriteLine($"Saving results...");

            // TO DO: WRITE FILE OUTPUT AND CHANGE BATCH PROCESSOR TO USE OUTPUTFILES

            //UploadContentToContainerAsync(outputFile, $"Aggregate result: {values.Average()}", outputContainer).GetAwaiter().GetResult();

            Console.WriteLine($"\nData processing finished!");
        }
    }
}
