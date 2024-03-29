# Big data Scala + Spark project


## Table of contents
- [Task content](#task-content)
- [Requirements](#requirements)
- [Project prepare instruction](#project-prepare-instruction)
- [Running project tasks](#running-project-tasks)
- [Documenting the solution](#documenting-the-solution)
- [Status](#status)
- [Contact](#contact)

## Task content 

###  Given datasets
####  Mobile App click stream projection

Schema:
* userId: String
* eventId: String
* eventTime: Timestamp
* eventType: String
* attributes: Map[String, String]

It include events of the following types that form a user engagement session:

* app_open
* search_product
* view_product_details
* purchase
* app_close

####  Purchases projection

Schema:
* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean

### Tasks & Requirements

####  Tasks #1 - Build Purchases Attribution Projection

Target schema:

* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean
* sessionId: String // a session starts with app_open event and finishes with app_close
* campaignId: String  // derived from app_open#attributes#campaign_id
* channelIid: String    // derived from app_open#attributes#channel_id

####  Tasks #2

Target: <br />

#2.1. Top Campaigns:
* Top 10 marketing campaigns that bring the biggest revenue <br />

#2.2. Channels engagement performance:
* Most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)<br />

####  Tasks #3

Target: <br />

#3.1. Compare performance on top CSV input and parquet input. <br />

#3.2. Metrics from Task #2 for different time periods:
* For September 2020
* For 2020-11-11 <br />
With performance on top csv input and partitioned parquet input. <br />

## Requirements
* Git
* Python 3.8 (or higher) and pip3 (package-management system)

## Project prepare instruction
To prepare project, follow these steps: <br />
1. Open terminal and clone the project from github repository:
```
$ git clone https://github.com/mkrolczyk12/scala-spark.git
```
```
$ cd <project_cloned_folder>
```
where <project_cloned_folder> is a path to project root directory <br />

2. (Optional) Create and activate virtualenv for installing data generation script requirements: <br />
* If no virtualenv package installed, run:
```
$ python3 -m pip install --upgrade pip
$ pip3 install virtualenv
```   
* Then
```
$ python3 -m venv ENV_NAME
```
where 'ENV_NAME' is the name of env
* Activate virtualenv
```
$ source ./ENV_NAME/bin/activate
```

3. Generate sample input data for your project:
* Install the required python packages and run the main script from the bigdata-input-generator directory to generate
  the datasets
```
// if step 2 wasn't omitted
(ENV_NAME)$ pip3 install -r ./bigdata-input-generator/requirements.txt
(ENV_NAME)$ python3 ./bigdata-input-generator/main.py
// else
$ pip3 install -r ./bigdata-input-generator/requirements.txt
$ python3 ./bigdata-input-generator/main.py
```

4. (Optional) Open 'application.conf' file and adjust project settings to your needs

5. Run following commands from project root directory:
```
$ sbt clean
$ sbt compile
```
Project should be ready to use.

## Running project tasks
General run command schema is:
```
$ sbt "run <task_option> <path_to_project_config_file> <task_specific_option1> <task_specific_option2> ..."
```
where:
* <task_option> is a type of task to run (possible options: "task1", "task2", "task3-metric", "task3-weekly")
* <path_to_project_config_file> is a path to project config file <br />
* <task_specific_option1>, <task_specific_option2> (...) are arguments for selected task <br /> <br />

To start particular task, use following commands:
### Task 1:
```
$ sbt "run task1 <path_to_project_config_file>"
```
where <path_to_project_config_file> is a path to project config file <br /> <br />

Example (from project root directory): 
```
$ sbt "run task1 ./application.conf"
```

### Task 2:
```
$ sbt "run task2 <path_to_project_config_file>"
```
where <path_to_project_config_file> is a path to project config file <br /> <br />

Example (from project root directory):
```
$ sbt "run task2 ./application.conf"
```

### Task 3:
```
$ sbt "run task3-metric <path_to_project_config_file> <metric_type>"
```
where:
* <path_to_project_config_file> is a path to project config file <br />
* <metric_type> is the name of metric to calculate <br /> <br />

List of available metrics: <br />
* "t1-csv" - task 1 with csv input
* "t1-csv-udaf" - task 1 with csv input and udaf functions
* "t1-parquet" - task 1 with parquet input
* "t1-parquet-udaf" - task 1 with parquet input and udaf functions
* "t2-csv-revenue" - task 2 revenue with csv input
* "t2-csv-engagement" - task 2 engagement with csv input
* "t2-parquet-revenue" - task 2 revenue with parquet input
* "t2-parquet-engagement" - task 2 engagement with parquet input
* "t3-csv-revenue-sept" - revenue with csv input for September
* "t3-csv-engagement-sept" - engagements with csv input for September
* "t3-parquet-revenue-sept" - revenue with parquet input for September
* "t3-parquet-engagement-sept" - engagement with parquet input for September
* "t3-csv-revenue-day" - revenue with csv input for 2020-11-11
* "t3-csv-engagement-day" - engagements with csv input for 2020-11-11
* "t3-parquet-revenue-day" - revenue with parquet input for 2020-11-11
* "t3-parquet-engagement-day" - engagement with parquet input for 2020-11-11

Example (from project root directory):
```
$ sbt "run task3-metric ./application.conf t1-parquet" 
```
<br />

#### Weekly purchases projection per quarter: 
```
$ sbt "run task3-weekly <path_to_project_config_file> <year_number> <quarter_of_year>"
```
where:
* <path_to_project_config_file> is a path to project config file <br />
* <year_number> is a year from which we want to get the data <br />
* <quarter_of_year> is a quarter of year (possible options: "1", "2", "3", "4" or "all") <br /> <br />

Example (from project root directory):
```
$ sbt "run task3-weekly ./application.conf 2020 4"
```

## Documenting the solution

### Task #1 - Purchases Attribution Projection Dataframe (10 example rows)
purchaseId | purchaseTime | billingCost | isConfirmed | sessionId | campaignId | channelId
--- | --- | --- | --- |--- |--- |--- 
|9363f61f-748c-4174-a4f1-577238fb001e|2020-11-02 14:04:35|504.03     |true       |100010   |604       |VK Ads      |
|f5314027-15c3-4fbf-9e8d-1ded00852929|2020-10-13 02:46:05|920.78     |true       |100140   |380       |Twitter Ads |
|b003d900-81b3-4c86-bec3-b55a63f5b1d7|2020-12-05 07:06:49|224.1      |true       |100227   |604       |Twitter Ads |
|d15c13f2-eb3d-44a7-95cf-abd1d6871523|2020-12-15 01:46:37|54.0       |true       |100263   |380       |Google Ads  |
|2babb929-8610-4f72-a51e-d4cc44bf17cd|2020-09-30 03:04:08|111.53     |false      |100320   |380       |Google Ads  |
|11e9c7a4-372d-4109-9ed1-63f316f112e7|2020-11-24 07:55:19|453.97     |false      |100553   |380       |Facebook Ads|
|e1148833-5b01-46d3-934c-c2c8611d0c8a|2020-11-16 13:42:25|943.73     |true       |100704   |380       |Twitter Ads |
|14bf5f33-74f9-4a6d-ada2-50c61b9cac8d|2020-12-07 01:13:18|181.52     |false      |100735   |544       |Yandex Ads  |
|7237115e-468d-444b-90c1-c3695ccd52ef|2020-12-14 23:26:38|146.73     |false      |100768   |544       |Google Ads  |
|acca4fd0-07f9-4d04-b9b0-c36c4342a738|2020-11-15 11:22:28|281.58     |false      |10096    |930       |Yandex Ads  |

### Task #2.1 - Top 10 marketing campaigns that bring the biggest revenue
campaignId | revenue
--- | ---
|190       |2041060.84|
|528       |1510378.16|
|325       |1470622.32|
|585       |1047838.02|
|779       |1031561.14|
|650       |1031403.13|
|859       |1024151.66|
|610       |1020675.58|
|669       |1019272.57|
|461       |1018971.02|

### Task #2.2 - The most popular channel that drives the highest amount of unique sessions (engagements) in each campaign (10 example rows)
campaignId | channelId | unique_sessions
--- | --- | ---
|829       |Facebook Ads|416            |
|451       |Yandex Ads  |821            |
|853       |Yandex Ads  |828            |
|666       |Google Ads  |413            |
|124       |Twitter Ads |432            |
|475       |Yandex Ads  |415            |
|574       |Yandex Ads  |851            |
|205       |Facebook Ads|407            |
|334       |Twitter Ads |416            |
|544       |Yandex Ads  |436            |

### Task #3.1 - Compare performance on top CSV input and parquet input

#### For Task 1

Option | Csv input | Parquet input
--- | --- | ---
normal | 94.971s | 31.406s
with custom UDAF | 89.573s | 32.021s

#### For Task 2

Top 10 marketing campaigns that bring the biggest revenue

Csv input | Parquet input
--- | ---
111.296s | 53.973s

The most popular channel that drives the highest amount of unique sessions in each campaign

Csv input | Parquet input
--- | ---
114.485s | 61.369s

### Task #3.2 - Calculate metrics from Task #2 for different time periods

#### Top 10 marketing campaigns that bring the biggest revenue
* For September 2020 <br />

Csv input | Parquet input
--- | ---
93.290s | 32.382s

* For 2020-11-11 <br />

Csv input | Parquet input
--- | ---
85.468s | 18.459s

#### the most popular channel that drives the highest amount of unique sessions in each campaign
* For September 2020 <br />

Csv input | Parquet input
--- | ---
108.667s | 38.060s

* For 2020-11-11 <br />

Csv input | Parquet input
--- | ---
84.794s | 21.647s

### Task #3.3 - Weekly purchases Projection within one quarter (10 example rows for Q4 of 2020)
purchaseId | purchaseTime | billingCost | isConfirmed | sessionId | campaignId | channelId | year | quarter | weekOfYear
--- | --- | --- | --- |--- |--- |--- |--- |--- |---
|dbcdc78c-f235-40db-b07f-0e0b42e47259|2020-12-11 01:40:49|59.9       |false      |10096    |556       |Yandex Ads  |2020|4      |50        |
|78e4f2c4-1983-4e2a-b888-58c8af58f692|2020-12-11 17:06:52|894.3      |false      |10351    |384       |Yandex Ads  |2020|4      |50        |
|e34f3e08-1e0f-4b9f-a1ce-b9260d2a38dd|2020-12-11 05:14:07|423.49     |true       |10436    |291       |VK Ads      |2020|4      |50        |
|94279274-da40-4441-b776-12a864d5e6f9|2020-10-12 04:40:36|906.03     |true       |1090     |604       |VK Ads      |2020|4      |42        |
|8eb71f01-5473-4138-ae06-8466ab3b4c51|2020-12-11 03:15:58|104.66     |true       |11078    |998       |VK Ads      |2020|4      |50        |
|1d3d9a6e-c104-42ff-a018-d52ddb8a5c7d|2020-12-11 06:08:07|408.89     |true       |11332    |252       |Google Ads  |2020|4      |50        |
|0bc7685a-4189-475e-aea8-8b8c3f698167|2020-11-19 20:16:32|877.99     |true       |11563    |828       |Twitter Ads |2020|4      |47        |
|29a41a2f-3a35-48c2-836d-c190d61c6582|2020-10-12 14:38:27|119.42     |false      |1159     |266       |VK Ads      |2020|4      |42        |
|703e424a-c007-4964-ac19-81c628e546c6|2020-11-19 15:35:57|983.96     |false      |11722    |269       |Twitter Ads |2020|4      |47        |
|49504b73-e4ac-4b04-80d9-cbd877367afa|2020-11-19 10:50:29|755.76     |true       |11888    |819       |Twitter Ads |2020|4      |47        |

## Status

_completed_

## Contact

Created by @mkrolczyk12 - feel free to contact me!

- E-mail: m.krolczyk66@gmail.com
