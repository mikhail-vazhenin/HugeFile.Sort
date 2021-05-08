using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace HugeFile.Sort.Extensions
{
    public static class DataflowBlockExtensions
    {
        public static async Task<IReadOnlyCollection<TItem>> ReceiveAllData<TItem>(this ISourceBlock<TItem> sourceBlock)
        {
            var items = new List<TItem>();
            while (await sourceBlock.OutputAvailableAsync())
            {
                items.Add(await sourceBlock.ReceiveAsync());
            }

            return items.AsReadOnly();
        }

        public static async Task SendAllData<TItem>(this ITargetBlock<TItem> targetBlock, IEnumerable<TItem> items)
        {
            foreach (var item in items)
            {
                await targetBlock.SendAsync(item);
            }
            targetBlock.Complete();
        }
    }
}
