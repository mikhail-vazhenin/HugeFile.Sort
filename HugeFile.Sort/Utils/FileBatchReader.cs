using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HugeFile.Sort.Utils
{
    public class FileBatchReader : IEnumerable<string[]>
    {
        private readonly string _path;
        private readonly long _batchSize;

        public FileBatchReader(string path, long batchSize)
        {
            _path = path;
            _batchSize = batchSize;
        }

        public IEnumerator<string[]> GetEnumerator()
        {
            using var streamReader = new StreamReader(_path);
            var buffer = new List<string>();
            var lineIndex = 0;
            Stopwatch stopwatch = Stopwatch.StartNew();

            long currentSize = 0;
            while (!streamReader.EndOfStream)
            {
                var line = streamReader.ReadLine();
                lineIndex++;

                if (currentSize + line.Length > _batchSize)
                {
                    Console.WriteLine($"[{DateTime.Now}] Partition read. Elapsed: {stopwatch.Elapsed}");
                    yield return buffer.ToArray();
                    stopwatch.Restart();
                    buffer.Clear();
                    currentSize = 0;
                }

                buffer.Add(line);
                currentSize += line.Length;
            }

            yield return buffer.ToArray();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}
