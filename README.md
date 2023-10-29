# agg_using_parquet4cs
C#でparquetを使用した集計処理のサンプル


## requirement
- workspaces.csprojを参照
``` XML
  <ItemGroup>
    <PackageReference Include="Csv" Version="2.0.93" />
    <PackageReference Include="Parquet.Net" Version="4.16.4" />
    <PackageReference Include="YamlDotNet" Version="13.7.1" />
  </ItemGroup>
```

## description
- 集計処理の指示はsetting.yamlでおこないます
    ``` yaml
    - id: なんでもOK
      description: この集計指示の概要を記載
      src: data/src/inp_100000_col4.csv
      dest: data/dest/out_100000_col4_csv_Ken_Sei_Age.csv
      dimension:
        - name: 分類事項として使用する列名１
          position: データ中の位置
        - name: 分類事項として使用する列名２
          position: データ中の位置
      measure:
        - name: 集計事項として使用する列名
          position: データ中の位置
    ```
- 集計結果はログとして出力されます
- 各種ファイルの配置場所は以下で
  ``` shell
  data
  ├── log_yyyyMMdd_HHmmss.csv
  └── setting.yaml
  ```

## quickstart
- dockerで実行可能
``` sh
cd agg_using_parquet4cs
docker-compose up 
```

## memo
- CSV > parquetの処理がありますがCSVデータの構造をclassで定義する形式で使いづらいです
  - Parquet.Netの「[Writing with low level API](https://aloneguid.github.io/parquet-dotnet/starter-topic.html#writing-with-low-level-api)」を参考に修正する必要あり
- Parquetは元データが複数列あっても必要な列のみを読み込むことができるため、列数が多い（＝ファイルサイズが大きい）データに対して、前処理分の処理時間が必要なくなるのがメリットか
