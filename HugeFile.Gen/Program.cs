using System;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace HugeFile.Gen
{
    class Program
    {
        static long availableMemory = (long)(Math.Pow(2, 20) * 2000);
        static long _fileSize = 1000;// (long)(Math.Pow(2, 20) * 10000);
        static int magicConstant = 8;

        static int fileMaxNum = 1000000;
        static int fileWordCount = 3;


        static string[] words = new string[]{"lorem", "ipsum", "dolor", "sit", "amet", "consectetuer",
        "adipiscing", "elit", "sed", "diam", "nonummy", "nibh", "euismod",
        "tincidunt", "ut", "laoreet", "dolore", "magna", "aliquam", "erat"};

        static async Task Main(string[] args)
        {
            var stopwatch = Stopwatch.StartNew();
            var fileName = $"./random_{DateTime.Now:HHmmss}.txt";

            var batchSize = Math.Min(_fileSize, availableMemory / magicConstant);

            BufferBlock<string> queue = new BufferBlock<string>(new DataflowBlockOptions { BoundedCapacity = 1 });

            var producer = Producer(queue, batchSize, _fileSize);
            var consumer = Consumer(queue, fileName);

            await Task.WhenAll(producer, consumer);

            Console.WriteLine($"Done! Elapsed:{stopwatch.Elapsed}");
        }
        private static async Task Producer(BufferBlock<string> queue, double blockSize, double fileSize)
        {
            long currentSize = 0;
            while (currentSize < fileSize)
            {
                var str = CreateBlock(blockSize);

                await queue.SendAsync(str);
                currentSize += str.Length;
                Console.WriteLine($"New block created");
            }
            queue.Complete();
        }

        private static async Task Consumer(BufferBlock<string> queue, string fileName)
        {
            using var writer = new StreamWriter(fileName, false, Encoding.UTF8, 65536);

            while (await queue.OutputAvailableAsync())
            {
                var data = await queue.ReceiveAsync();
                writer.Write(data);

                Console.WriteLine($"Block saved");
                GC.Collect();
            }
        }

        static string CreateBlock(double blockSize)
        {
            Random random = new Random((int)DateTime.Now.Ticks);
            var builder = new StringBuilder((int)blockSize);

            while (builder.Length < builder.Capacity)
            {
                builder
                    .Append(random.Next(0, fileMaxNum))
                    .Append(". ");

                var wordCount = random.Next(1, fileWordCount);
                for (int w = 0; w < wordCount; w++)
                {
                    if (w > 0) { builder.Append(" "); }
                    builder.Append(words[random.Next(words.Length)]);
                }
                builder.AppendLine();
            }

            return builder.ToString();
        }

    }
}
