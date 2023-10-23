# agg_using_parquet4cs
C#でparquetを使用した集計処理のサンプル

## description
1. inp.csv を parquet形式に変換します。(inp.parquetが作成されます)
2. 作成したparquet形式のファイルを読み込んで集計処理をします。

## quickstart
``` sh
docker-compose up 
$ Starting dotnet7-dev ... done
$ Attaching to dotnet7-dev
$ dotnet7-dev     | /workspaces/Porgram.cs(14,23): warning CS8618: null 非許容の プロパティ 'Ken' には、コンストラクターの終了時に null 以外の値が入っていなければなりません。プロパティ を Null 許容として宣言することをご検討ください。 [/workspaces/workspaces.csproj]
$ dotnet7-dev     | /workspaces/Porgram.cs(15,23): warning CS8618: null 非許容の プロパティ 'Rou' には、コンストラクターの終了時に null 以外の値が入っていなければなりません。プロパティ を Null 許容として宣言することをご検討ください。 [/workspaces/workspaces.csproj]
$ dotnet7-dev     | /workspaces/Porgram.cs(16,23): warning CS8618: null 非許容の プロパティ 'Sei' には、コンストラクターの終了時に null 以外の値が入っていなければなりません。プロパティ を Null 許容として宣言することをご検討ください。 [/workspaces/workspaces.csproj]
$ dotnet7-dev     | == Parquet Exists. ================
$ dotnet7-dev     |  00:00:00.0052242 
$ dotnet7-dev     |  0時間 0分 0秒 5ミリ秒
$ dotnet7-dev     |  5ミリ秒
$ dotnet7-dev     | ken   count   min     max     mean
$ dotnet7-dev     | 46    5       2       33087   1       99      50.27871973887025
$ dotnet7-dev     | 3     5       1       33022   1       99      50.25855490279208
　　・
　　・
　　・
　　・
　　・
$ dotnet7-dev     | 36    7       1       33368   1       99      50.05247542555742
$ dotnet7-dev     | 13    3       1       33490   1       99      50.256225739026576
$ dotnet7-dev     | == Using Parquet Summary ================
$ dotnet7-dev     |  00:00:27.6413182 
$ dotnet7-dev     |  0時間 0分 27秒 641ミリ秒
$ dotnet7-dev     |  27641ミリ秒
$ dotnet7-dev exited with code 0

```

- 表示されるログの内容は以下

| log_text | description |
| --------- | ---------- |
| **Parquet Exists.** | Parquetファイルが既に存在したので変換処理をしなかったよのログ |
| **Parquet Not Exists. Go CSV to Parquet !!** | CSVファイルからParquetファイルに変換した時の処理時間 |
| **Using Parquet Summary** | Parquetファイルを使用した集計処理の処理時間 |

## TODO
- 入力ファイルを外から渡せるようにする
- 処理に使用しているデータクラスを動的に定義できるようにする
- 処理結果を出力できるようにする
- LINQで準備されていない標準偏差等に対応する
