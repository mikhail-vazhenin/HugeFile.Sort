using System;
using System.Collections.Generic;
using System.Text;

namespace HugeFile.Sort.Comparers
{
    public class LineComparer : IComparer<string>
    {
        public int Compare(string x, string y)
        {
            var xspan = x.AsSpan();
            var xpI = xspan.IndexOf('.');

            var yspan = y.AsSpan();
            var ypI = yspan.IndexOf('.');

            var compare = xspan.Slice(xpI + 1, xspan.Length - xpI - 1).CompareTo(yspan.Slice(ypI + 1, yspan.Length - ypI - 1), StringComparison.Ordinal);//.Compare(partsX[1], partsY[1], StringComparison.Ordinal);

            if (compare == 0)
            {
                compare = int.Parse(xspan.Slice(0, xpI)).CompareTo(int.Parse(yspan.Slice(0, ypI)));
            }
            if (compare == 0) return 1;

            return compare;
        }
    }
}
