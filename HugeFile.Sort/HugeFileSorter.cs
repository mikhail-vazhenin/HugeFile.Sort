using HugeFile.Sort.Comparers;
using HugeFile.Sort.Models;
using HugeFile.Sort.Utils;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using HugeFile.Sort.Extensions;

namespace HugeFile.Sort
{
    public class HugeFileSorter
    {
        private readonly string _sourceFileName;
        private readonly string _destFileName;
        private readonly SortConfig _sortConfig;
        private const string _splitsFolder = "./splits";
        private const string _mergeFolder = "./merge";

        public HugeFileSorter(string sourceFileName, string destFileName, SortConfig sortConfig = null)
        {
            _sourceFileName = sourceFileName;
            _destFileName = destFileName;
            _sortConfig = sortConfig ?? new SortConfig();
        }

        public async Task Sort()
        {

            Console.WriteLine($"[{DateTime.Now}] Read file {Path.GetFileName(_sourceFileName)}");

            Stopwatch stopwatch = Stopwatch.StartNew();

            var fileSize = new FileInfo(_sourceFileName).Length;
            var maxDegreeOfParallelism = _sortConfig.MaxDegreeOfParallelismPerLevel;

            var partitionSize = EstimateSizeOfPartition(_sortConfig.ApproximateMemoryLimit, maxDegreeOfParallelism);

            if (Directory.Exists(_splitsFolder)) Directory.Delete(_splitsFolder, true);
            Directory.CreateDirectory(_splitsFolder);
            if (Directory.Exists(_mergeFolder)) Directory.Delete(_mergeFolder, true);
            Directory.CreateDirectory(_mergeFolder);

            var batchReader = new BufferBlock<BatchInfo>(new DataflowBlockOptions { BoundedCapacity = (int)(maxDegreeOfParallelism * 0.1 + 1) });
            var sorterBlock = new TransformBlock<BatchInfo, BatchInfo>(l => PartitionSorter(l),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism, BoundedCapacity = maxDegreeOfParallelism });
            var batchSaverBlock = new TransformBlock<BatchInfo, string>(l => PartitionSaver(l),
                new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism, BoundedCapacity = maxDegreeOfParallelism });

            var linkOptions = new DataflowLinkOptions { PropagateCompletion = true };
            batchReader.LinkTo(sorterBlock, linkOptions);
            sorterBlock.LinkTo(batchSaverBlock, linkOptions);

            var partitioner = Partitioner(_sourceFileName, partitionSize, batchReader);
            var filesForMerge = await batchSaverBlock.ReceiveAllData();

            await partitioner;
            await batchSaverBlock.Completion;

            Console.WriteLine($"[{DateTime.Now}] File splitted. Elapsed: {stopwatch.Elapsed}");

            BatchBlock<string> sortedBufferBlock = null;
            TransformBlock<IEnumerable<string>, string> mergerBlock = null;

            while (filesForMerge.Count > 1)
            {
                var estimatedCacheSize = EstimateSizeOfCache(_sortConfig.ApproximateMemoryLimit, filesForMerge.Count, _sortConfig.MaxDegreeOfParallelismPerLevel);
                var batchBlockSize = filesForMerge.Count / _sortConfig.MaxDegreeOfParallelismPerLevel;
                if (batchBlockSize == 1) batchBlockSize = 2;
                if (batchBlockSize == 0) batchBlockSize = _sortConfig.MaxDegreeOfParallelismPerLevel;

                sortedBufferBlock = new BatchBlock<string>(batchBlockSize);
                mergerBlock = new TransformBlock<IEnumerable<string>, string>(l => Merge(l, estimatedCacheSize),
                    new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism, BoundedCapacity = maxDegreeOfParallelism });

                sortedBufferBlock.LinkTo(mergerBlock, linkOptions);

                await sortedBufferBlock.SendAllData(filesForMerge);
                filesForMerge = await mergerBlock.ReceiveAllData();

                await mergerBlock.Completion;
            }

            if (filesForMerge.Count == 0) Console.WriteLine("Somthing went wrong. Result file is not created");
            else
            {
                File.Move(filesForMerge.ElementAt(0), _destFileName, true);
                Console.WriteLine($"[{DateTime.Now}] Order complete. Result file: {_destFileName}. Elapsed: {stopwatch.Elapsed}");
            }
        }

        public static long EstimateSizeOfPartition(long memoryLimit, int maxDegreeOfParallelism)
        {
            return (long)(memoryLimit / (maxDegreeOfParallelism * 1.3) / Constants.MagicConstant);
        }

        public static long EstimateSizeOfCache(long memoryLimit, int fileCount, int maxDegreeOfParallelismPerLevel)
        {
            return (long)(memoryLimit / (fileCount + maxDegreeOfParallelismPerLevel) / Constants.MagicConstant);
        }

        private static async Task Partitioner(string file, long batchSize, BufferBlock<BatchInfo> queue)
        {
            var fileBatchReader = new FileBatchReader(file, batchSize);

            int fileIndex = 0;
            foreach (var batch in fileBatchReader)
            {
                await queue.SendAsync(new BatchInfo { Index = fileIndex++, Lines = batch });
            }

            queue.Complete();
        }

        private static BatchInfo PartitionSorter(BatchInfo batchInfo)
        {
            Console.WriteLine($"[{DateTime.Now}] Start sorting {batchInfo.Index}");

            Stopwatch stopwatch = Stopwatch.StartNew();
            var comparer = new LineComparer();
            Array.Sort(batchInfo.Lines, comparer);

            Console.WriteLine($"[{DateTime.Now}] Partition sorted {batchInfo.Index}. Elapsed: {stopwatch.Elapsed}");

            return batchInfo;
        }

        private string PartitionSaver(BatchInfo batchInfo)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();

            var batchFileName = $"{_splitsFolder}/part_{batchInfo.Index:d5}.txt";

            using (var streamWriter = new StreamWriter(batchFileName))
            {
                foreach (var line in batchInfo.Lines) streamWriter.WriteLine(line);
                streamWriter.Close();
            }

            batchInfo.Lines = null;

            Console.WriteLine($"[{DateTime.Now}] Partition saved. Elapsed: {stopwatch.Elapsed}");
            return batchFileName;
        }

        public static string Merge(IEnumerable<string> sortedFiles, long cacheSize)
        {
            var files = sortedFiles.ToArray();
            if (files.Length == 1) return files[0];

            var resultFile = $"./{_mergeFolder}/{Path.GetRandomFileName()}";
            if (File.Exists(resultFile)) File.Delete(resultFile);

            int fileCount = files.Length;

            var iterators = new BufferedFileIterator[fileCount];
            for (var i = 0; i < fileCount; i++)
            {
                var iterator = new BufferedFileIterator(files[i], cacheSize);
                iterator.MoveNext();
                iterators[i] = iterator;
            }

            var comaprer = new LineComparer();
            var buffer = new List<string>();
            var currentSize = 0;

            while (true)
            {
                IEnumerator<string> minIterator = null;
                foreach (var iterator in iterators)
                {
                    if (iterator.Current != null)
                    {
                        if (minIterator == null || comaprer.Compare(minIterator.Current, iterator.Current) > 0)
                        {
                            minIterator = iterator;
                        }
                    }
                }

                if (minIterator == null) break;

                var minString = minIterator.Current;
                buffer.Add(minString);
                currentSize += minString.Length;
                minIterator.MoveNext();


                if (currentSize > cacheSize)
                {
                    File.AppendAllLines(resultFile, buffer);
                    buffer.Clear();
                    currentSize = 0;
                }
            }

            File.AppendAllLines(resultFile, buffer);

            foreach (var i in iterators) i.Dispose();

            return resultFile;
        }
    }
}
