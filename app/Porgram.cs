using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using Parquet.Serialization;
using System;
using System.IO;
using System.Linq;
using System.Net.WebSockets;

namespace ParquetSampleApp
{
    class Record {
        public String Ken { get; set; }
        public String Rou { get; set; }
        public String Sei { get; set; }
        public double Age { get; set; }
    }

    class Program
    {
        // static void Main(string[] args)
        // {

        //     Console.WriteLine("HOGE");

        // }

        static async Task Csv2Parquet(string src, string dst)
        {

            var datas = File.ReadLines(src)
                    .ToArray()
                    .Select(x => new Record{
                        Ken = x.Split(',')[0],
                        Rou = x.Split(',')[1],
                        Sei = x.Split(',')[2],
                        Age = double.Parse(x.Split(',')[3])
                    }).Skip(1).ToList();

            await ParquetSerializer.SerializeAsync(datas, dst);

        }

        static void ProcTime(String text, System.Diagnostics.Stopwatch sw)
        {
            Console.WriteLine($"== {text} ================");
            TimeSpan ts = sw.Elapsed;
            Console.WriteLine($" {ts} ");
            Console.WriteLine($" {ts.Hours}時間 {ts.Minutes}分 {ts.Seconds}秒 {ts.Milliseconds}ミリ秒");
            Console.WriteLine($" {sw.ElapsedMilliseconds}ミリ秒");
        }

        static async Task Main(string[] args)
        {

            String ProcText = "";
            System.Diagnostics.Stopwatch sw = new();
            sw.Start();

            String csv_src = "inp.csv";
            // String csv_src = "testdata.csv";
            String parquet_src = Path.ChangeExtension(csv_src, "parquet");
            if (!Path.Exists(parquet_src)){
                await Csv2Parquet(csv_src, parquet_src);
                ProcText = "Parquet Not Exists. Go CSV to Parquet !!";
            } else {
                ProcText = "Parquet Exists.";
            }
            sw.Stop();
            ProcTime(ProcText, sw);
            

            sw.Restart();

            using Stream fs = System.IO.File.OpenRead(parquet_src);
            IList<Record> data = await ParquetSerializer.DeserializeAsync<Record>(fs) ;

            var agg = data
                    .GroupBy(row => new {row.Ken, row.Rou, row.Sei})
                    .Select(g => new
                    {
                        key = g.Key,
                        count = g.Count(),
                        mean = g.Average(row => row.Age),
                        min = g.Min(row => row.Age),
                        max = g.Max(row => row.Age)
                    });


            Console.WriteLine("ken\tcount\tmin\tmax\tmean");
            foreach (var group in agg)
            {
                Console.WriteLine($"{group.key.Ken}\t{group.key.Rou}\t{group.key.Sei}\t{group.count}\t{group.min}\t{group.max}\t{group.mean}");
            }

            sw.Stop();
            ProcText = "Using Parquet Summary";
            ProcTime(ProcText, sw);



        }



    }
}