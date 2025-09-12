# QuestSquirrel

[日本語はこちら](#プロジェクトについて)

## Table of Contents
- [QuestSquirrel](#questsquirrel)
  - [Table of Contents](#table-of-contents)
- [About this project](#about-this-project)
  - [About DAG](#about-dag)
- [Implementation Examples by Section](#implementation-examples-by-section)
  - [Production SQL](#production-sql)
  - [Query Task Implementation Example](#query-task-implementation-example)
  - [Report Task Implementation Example](#report-task-implementation-example)
  - [Interactive Dashboard Implementation Example](#interactive-dashboard-implementation-example)
- [プロジェクトについて](#プロジェクトについて)
  - [DAGについて](#dagについて)
- [各セクションの実装例](#各セクションの実装例)
  - [プロダクションSQL](#プロダクションsql)
  - [クエリタスク実装例](#クエリタスク実装例)
  - [レポートタスク実装例](#レポートタスク実装例)
  - [インタラクティブダッシュボード実装例](#インタラクティブダッシュボード実装例)
---

# About this project

Please check the article (*QuestSquirrel is Born!*):  
[https://zhunworld.com/en/article/10/5/2024/questsquirrel.html](https://zhunworld.com/en/article/10/5/2024/questsquirrel.html)

## About DAG

Please check the article:  
[https://zhunworld.com/en/article/8/2/2024/async-dag-task-scheduler.html](https://zhunworld.com/en/article/8/2/2024/async-dag-task-scheduler.html)

---

# Implementation Examples by Section

## Production SQL

All business SQL is stored in the `templates` directory, organized by task type, batch, iteration, and production stage.  
Through parameter filling, SQL can be reused across scenarios. Complex tasks may involve *long SQL* with:

- Multiple temporary tables  
- Multiple data sources  
- Multi-dimensional aggregations  
- Multiple joins  
- Multi-granularity calculations  
- Multiple output formats  
- Advanced data processing functions  

**Examples:**  
- Long SQL Case 1: [step1_push_overwrite.sql](https://github.com/zizhGuo/QuestSquirrel/blob/master/templates/request_2_1/iter_4/dev/step1_push_overwrite.sql)  
- Long SQL Case 2: [dws_envir_task3.sql](https://github.com/zizhGuo/QuestSquirrel/blob/master/templates/request_5_1/iter_2/test/dws_envir_task3.sql)

---

## Query Task Implementation Example

`Multiphase` base class implements the core logic for running query tasks.  
Subclasses extend it for specific business scaling.

- Base class: [qquest_base.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/multiphases/qquest_base.py)  
- Business subclass: [qquest_6_3_iter_1.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/multiphases/qquest_6_3_iter_1.py)

---

## Report Task Implementation Example

`Transform` base class provides the foundation for generating **xlsx** reports (using `openpyxl`, `pandas`),  
including helper functions. Subclasses extend the class to support advanced reporting:

- Cell formatting  
- Font color customization  
- Merging cells  
- Other complex operations  

- Base class: [base_transform.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/transform/base_transform.py)  
- Business subclass: [quest_6_2_iter_1.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/transform/quest_6_2_iter_1.py)

---

## Interactive Dashboard Implementation Example

`Visual` base class implements HTML visualization using **pyecharts**.  
Subclasses extend and add custom **JavaScript hacks** for callback functions.

- Base class: [visual_base.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/visual/visual_base.py)  
- Business subclass: [vquest_5_2_iter_1.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/visual/vquest_5_2_iter_1.py)



---

# プロジェクトについて

記事はこちらをご覧ください (*QuestSquirrel is Born!*):  
[https://zhunworld.com/en/article/10/5/2024/questsquirrel.html](https://zhunworld.com/en/article/10/5/2024/questsquirrel.html)

## DAGについて

記事はこちらをご覧ください:  
[https://zhunworld.com/en/article/8/2/2024/async-dag-task-scheduler.html](https://zhunworld.com/en/article/8/2/2024/async-dag-task-scheduler.html)

---

# 各セクションの実装例

## プロダクションSQL

すべての業務SQLは `templates` ディレクトリに保存されており、タスクの種類、バッチ、イテレーション、運用段階ごとに整理されています。  
パラメータを埋め込むことで再利用が可能です。複雑なタスクでは以下のような*長いSQL*が含まれる場合があります：

- 複数の一時テーブル  
- 複数のデータソース  
- 多次元集計  
- 複数のJOIN  
- 多粒度での計算  
- 複数の出力フォーマット  
- 高度なデータ処理関数  

**例:**  
- 長SQLケース1: [step1_push_overwrite.sql](https://github.com/zizhGuo/QuestSquirrel/blob/master/templates/request_2_1/iter_4/dev/step1_push_overwrite.sql)  
- 長SQLケース2: [dws_envir_task3.sql](https://github.com/zizhGuo/QuestSquirrel/blob/master/templates/request_5_1/iter_2/test/dws_envir_task3.sql)

---

## クエリタスク実装例

`Multiphase` 基底クラスはクエリタスクを実行する基本ロジックを実装しています。  
サブクラスはこれを継承し、具体的な業務のスケーリングに対応します。

- 基底クラス: [qquest_base.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/multiphases/qquest_base.py)  
- 業務サブクラス: [qquest_6_3_iter_1.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/multiphases/qquest_6_3_iter_1.py)

---

## レポートタスク実装例

`Transform` 基底クラスは **xlsx** レポートを生成するための基礎（`openpyxl`、`pandas`を利用）を提供し、  
複数のユーティリティ関数を含みます。サブクラスは拡張を行い、高度なレポート生成を実現します：

- セルの書式設定  
- フォント色のカスタマイズ  
- セル結合  
- その他複雑な操作  

- 基底クラス: [base_transform.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/transform/base_transform.py)  
- 業務サブクラス: [quest_6_2_iter_1.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/transform/quest_6_2_iter_1.py)

---

## インタラクティブダッシュボード実装例

`Visual` 基底クラスは **pyecharts** を用いたHTML可視化を実装しています。  
サブクラスはこれを拡張し、**JavaScriptによるコールバック関数のカスタムハック**を追加します。

- 基底クラス: [visual_base.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/visual/visual_base.py)  
- 業務サブクラス: [vquest_5_2_iter_1.py](https://github.com/zizhGuo/QuestSquirrel/blob/master/visual/vquest_5_2_iter_1.py)
