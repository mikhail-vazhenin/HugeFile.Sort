using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace HugeFile.Sort
{
    class Program
    {
        static async Task Main(string[] args)
        {
            if (args.Length == 2)
            {
                var sourceFile = args[0];
                var destFile = args[1];

                if (File.Exists(sourceFile))
                    await new HugeFileSorter(sourceFile, destFile).Sort();
                else
                    Console.WriteLine($"Source file with name {sourceFile} was not found");
            }
            else
            {
                Console.WriteLine($"Command line format `HugeFile.Sort.exe %sourceFile %destFile`");
            }
            Console.WriteLine("Press Enter to exit...");
            Console.ReadLine();
        }
    }
}
