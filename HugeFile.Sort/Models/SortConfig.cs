using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HugeFile.Sort.Models
{
    public class SortConfig
    {
        public long ApproximateMemoryLimit { get; set; } = (long)(Math.Pow(2, 20) * 2000);

        public int MaxDegreeOfParallelismPerLevel { get; set; } = Environment.ProcessorCount;
    }
}
