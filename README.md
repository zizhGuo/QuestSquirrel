# QuestSquirrel

## Table of Contents
- [QuestSquirrel](#questsquirrel)
  - [Table of Contents](#table-of-contents)
  - [About this project](#about-this-project)
    - [About DAG](#about-dag)
  - [Production SQL](#production-sql)
  - [Query Task Implementation Example](#query-task-implementation-example)
  - [Report Task Implementation Example](#report-task-implementation-example)
  - [Interactive Dashboard Implementation Example](#interactive-dashboard-implementation-example)

---

## About this project

Please check the article (*QuestSquirrel is Born!*):  
[https://zhunworld.com/en/article/10/5/2024/questsquirrel.html](https://zhunworld.com/en/article/10/5/2024/questsquirrel.html)

### About DAG

Please check the article:  
[https://zhunworld.com/en/article/8/2/2024/async-dag-task-scheduler.html](https://zhunworld.com/en/article/8/2/2024/async-dag-task-scheduler.html)

---

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
