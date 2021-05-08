using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace HugeFile.Sort.Utils
{
    public class BufferedFileIterator : IEnumerator<string>
    {
        private readonly string _fileName;
        private readonly long _cacheSize;
        private readonly List<string> _buffer;
        IEnumerator<string> _enumerator;

        readonly StreamReader _reader;

        public string Current => _enumerator.Current;
        object IEnumerator.Current => Current;


        public BufferedFileIterator(string fileName, long cacheSize = 10000)
        {
            _buffer = new List<string>();
            _enumerator = _buffer.GetEnumerator();
            _enumerator.MoveNext();
            _reader = new StreamReader(fileName);
            _fileName = fileName;
            _cacheSize = cacheSize;
        }

        private bool TryFillBuffer()
        {
            int currentSize = 0;
            _buffer.Clear();
            while (!_reader.EndOfStream && currentSize < _cacheSize)
            {
                var line = _reader.ReadLine();
                _buffer.Add(line);
                currentSize += line.Length;
            }
            _enumerator = _buffer.GetEnumerator();
            return _enumerator.MoveNext();
        }

        public bool MoveNext()
        {
            return _enumerator.MoveNext() || TryFillBuffer();
        }
        public void Reset() => _enumerator.Reset();

        public void Dispose()
        {
            _enumerator.Dispose();
            _reader.Close();
            File.Delete(_fileName);
        }
    }
}
