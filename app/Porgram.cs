using Parquet;
using Parquet.Data;
using Parquet.Serialization;
using Csv;
using YamlDotNet.RepresentationModel;
using System.Reflection;


namespace ParquetSampleApp
{


    class Csv2Parquet<T>
    {

        public async Task Convert(string src, string dest)
        {
            
            // 事前にクラスを作成しておく必要あり
            // Writing with low level API のコードを参考に修正する必要あり
            Type type = typeof(T);
            ConstructorInfo? ctor = type.GetConstructor(new Type[] { typeof(string[])});

            if (ctor is not null)
            {
                var datas = File.ReadLines(src)
                        .ToArray()
                        .Select(x => (T)ctor.Invoke(new object[] { x.Split(",") })
                        ).Skip(1).ToList();

                await ParquetSerializer.SerializeAsync(datas, dest);

            }

        }

    }
 

    class SumSource
    {

        public SumSource(String dimension, double measure)
        {
            this.Dimension = dimension;
            this.Measure = measure;
        }

        public String Dimension { get; set;} 
        public Double Measure { get; set;} 

    }


    class ColumnInfo
    {
        public ColumnInfo(String name, int position)
        {
            this.Name = name;
            this.Position = position;
        }

        public String Name { get; set;}
        public int Position {get; set;}

    }

    class StatTable
    {



      
        private static void SumCore(IEnumerable<SumSource> source, string dest, string[] dimensions)
        {

                var results = source
                    .GroupBy(item => new { item.Dimension })
                    .Select(g => new
                    {
                        dimension = g.Key,
                        count = g.Count(),
                        mean = g.Average(item => item.Measure),
                        min = g.Min(item => item.Measure),
                        max = g.Max(item => item.Measure)
                    })
                    .Select(row => {

                        
                        List<string> results_list = new List<string>(row.dimension.Dimension.Split("_"))!;

                        results_list.AddRange(new List<string>{
                                                        row.count.ToString(),
                                                        row.min.ToString(),
                                                        row.max.ToString(),
                                                        row.mean.ToString(),
                                                    });

                        return results_list.ToArray();

                    });



                List<string> dimensions_list = new List<string>(dimensions)!;

                dimensions_list.AddRange(new List<string>{
                                                "count",
                                                "min",
                                                "max",
                                                "mean",
                                            });


                string[] header = dimensions_list.ToArray();


                string txtcsv = CsvWriter.WriteToText(header, results);
                File.WriteAllText(dest, txtcsv);

        }

        public static void SumCsv(
                        String src,
                        String dest,
                        List<ColumnInfo> dimensions,
                        ColumnInfo measure,
                        Boolean isFirstRowHeader = true,
                        Char delimiter = ','
                        )

            {

            Dictionary<string, (int, double)> rowData = new();


            var options = new CsvOptions // Defaults
            {
                Comparer = StringComparer.OrdinalIgnoreCase
            };

            using var reader = new FileStream(src, FileMode.Open);

            IEnumerable<SumSource> result = CsvReader.ReadFromStream(reader, options)
                .Select(line => {
 
                    SumSource item = new(
                                    string.Join('_',
                                                dimensions
                                                .Select(dimension => line[dimension.Name])
                                                ),
                                    double.Parse(line[measure.Name])
                                    );
                    return item;
                });

            SumCore(result, dest, dimensions.Select(dimension => dimension.Name).ToArray());

        }

        public static async Task SumPar(String src, String dest, string[] dimensions, string measure)
        {

            // TODO RowGroup別にデータ取得する処理しないとRowGroupが複数あった場合に正しく処理されない
            //      このプログラムではRowGroupが1つしかない前提のプログラムとなっていることに注意
            using Stream fs1 = File.OpenRead(src);
            using ParquetReader reader = await ParquetReader.CreateAsync(fs1);
            for (int i = 0; i < reader.RowGroupCount; i++)
            {
                using ParquetRowGroupReader rowGroupReader = reader.OpenRowGroupReader(i);
                IEnumerable<DataColumn> columnDatas = await Task.WhenAll(
                                    reader.Schema.GetDataFields()
                                    .Where(df => dimensions.Contains(df.Name) || measure == df.Name)
                                    .Select(async df => await rowGroupReader.ReadColumnAsync(df)))
                                    ;


                Dictionary<string, Array> rowData = columnDatas
                                                    .ToDictionary(columnData => columnData.Field.Name, columnData => columnData.Data);

                IEnumerable<SumSource> result = Enumerable.Range(0, (int)rowGroupReader.RowCount)
                    .Select(index => {

                        SumSource item = new(
                                        string.Join('_',
                                                    dimensions
                                                    .Select(dimension => rowData[dimension].GetValue(index))
                                                    ),
                                        (double)rowData[measure].GetValue(index)!
                                        );
                        return item;

                    });

                SumCore(result, dest, dimensions);

            }


        }
    }

    partial class Program
    {



        static async Task Main(string[] args)
        {

            string setting_path = args[0];
            Console.WriteLine(setting_path);

            var input = new StreamReader(setting_path);
            var yaml = new YamlStream();
            yaml.Load(input);


            List<string[]> logs = new();


            var rootNode = yaml.Documents[0].RootNode;
            foreach (YamlNode setting in (YamlSequenceNode)rootNode){

                string id = (String)setting["id"]!;
                string description = (String)setting["description"]!;
                string src = (String)setting["src"]!;
                string dest = (String)setting["dest"]!;

                List<ColumnInfo> dimensions = ((YamlSequenceNode)setting["dimension"])
                                                .Select(dimension => {
                                                    ColumnInfo info = new(
                                                        (string)dimension["name"]!,
                                                        int.Parse((string)dimension["position"]!)
                                                        );
                                                    return info;
                                                })
                                                .ToList();

                List<ColumnInfo> measures = ((YamlSequenceNode)setting["measure"])
                                                .Select(measure => {
                                                    ColumnInfo info = new(
                                                        (string)measure["name"]!,
                                                        int.Parse((string)measure["position"]!)
                                                        );
                                                    return info;
                                                })
                                                .ToList();


                string ext = Path.GetExtension(src);

                var sw = new System.Diagnostics.Stopwatch();
                sw.Start();

                if (string.Compare(ext, ".csv", true) == 0){

                    StatTable.SumCsv(
                        src, dest, dimensions, measures[0], true);

                } else if (string.Compare(ext, ".parquet", true) == 0){

                    await StatTable.SumPar(
                        src, dest,
                        dimensions.Select(dimension => dimension.Name).ToArray(),
                        measures[0].Name);


                }

                sw.Stop();
                TimeSpan ts = sw.Elapsed;

                logs.Add(new string[]{
                    id, description,
                    $"{ts}",
                    $"{ts.Hours}時間 {ts.Minutes}分 {ts.Seconds}秒 {ts.Milliseconds}ミリ秒",
                    $"{sw.ElapsedMilliseconds}ミリ秒"
                });

            }

            String Now_String = DateTime.Now.ToString("yyyyMMdd_HHmmss");
            string log_path = $"data/log_{Now_String}.csv";
            string[] header = {"id","description", "timespan", "timespan", "timespan"};
            string logcsv = CsvWriter.WriteToText(header, logs);
            File.WriteAllText(log_path, logcsv);


        }
    }
}