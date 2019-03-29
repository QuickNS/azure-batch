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
            
            var values = new List<double>();
            var indexes = new List<int>();

            using (var reader = File.OpenText(filename))
            {
                while(!reader.EndOfStream)
                {
                    var line = reader.ReadLine();
                    var line_data = line.Split(',');

                    var index = line_data.First();
                    var value = line_data.Last();
                    Console.Write($"Processing line {index}...");
                    Task.Delay(delay).GetAwaiter().GetResult();
                    Console.WriteLine($"Result: {value}");
                    values.Add(Double.Parse(value));
                    indexes.Add(Int32.Parse(index));
                }
            }

            Console.WriteLine($"Saving results...");
            using (StreamWriter sw = new StreamWriter(outputFile, append: false))
            {
                var avg = values.Average();
                sw.WriteLine($"Aggregate result: {avg}\n");
                for(int i=0;i<values.Count;i++)
                {
                    sw.WriteLine($"Index {indexes[i]}: {values[i]}, {values[i]-avg}");
                }
            }
            Console.WriteLine($"\nData processing finished!");
        }
    }
}
