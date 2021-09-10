# GridU Introduction to BigData (Scala+Spark)
Scala + Spark capstone project


## Table of contents
- [Task content](#task-content)
- [Technologies and dependencies](#technologies-and-dependencies)
- [Requirements](#requirements)
- [Build instruction](#build-instruction)
- [Documenting the solution](#documenting-the-solution)
- [Status](#status)
- [Contact](#contact)

## Task content
### Domain
You work at a data engineering department of a company building an ecommerce platform. There is a mobile application that
is used by customers to transact with its on-line store. Marketing department of the company has set up various campaigns
(e.g. “Buy one thing and get another one as a gift”, etc.)  via different marketing channels (e.g. Google / Yandex / Facebook Ads,
etc.).

Now the business wants to know the efficiency of the campaigns and channels.

Let’s help them out!

### Dataset link:

https://github.com/gridu/INTRO_SPARK-SCALA_FOR_STUDENTS <br />

###  Given datasets
####  Mobile App click stream projection

Schema:
* userId: String
* eventId: String
* eventTime: Timestamp
* eventType: String
* attributes: Map[String, String]

There could be events of the following types that form a user engagement session:

* app_open
* search_product
* view_product_details
* purchase
* app_close

Events of app_open type may contain the attributes relevant to the marketing analysis:
* campaign_id
* channel_id

Events of purchase type contain purchase_id attribute.

####  Purchases projection

Schema:
* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean

### Tasks & Requirements

####  Tasks #1 - Build Purchases Attribution Projection

The projection is dedicated to enabling a subsequent analysis of marketing campaigns and channels. <br />

The target schema:

* purchaseId: String
* purchaseTime: Timestamp
* billingCost: Double
* isConfirmed: Boolean
* sessionId: String // a session starts with app_open event and finishes with app_close
* campaignId: String  // derived from app_open#attributes#campaign_id
* channelIid: String    // derived from app_open#attributes#channel_id

Requirements for implementation of the projection building logic: <br />

* Task #1.1 - Implement it by utilizing default Spark SQL capabilities. <br />
* Task #1.2 - Implement it by using a custom Aggregator or UDAF. <br />

####  Tasks #2 - Calculate Marketing Campaigns And Channels Statistics

Use the purchases-attribution projection to build aggregates that provide the following insights: <br />

Task #2.1. Top Campaigns:
* What are the Top 10 marketing campaigns that bring the biggest revenue (based on billingCost of confirmed purchases)? <br />

Task #2.2. Channels engagement performance:
* What is the most popular (i.e. Top) channel that drives the highest amount of unique sessions (engagements)  with the App in each campaign? <br />

Requirements for task #2:

* Should be implemented by using plain SQL on top of Spark DataFrame API
* Will be a plus: an additional alternative implementation of the same tasks by using Spark Scala DataFrame / Datasets  API only (without plain SQL)

####  Tasks #3 - (Optional) - Organize data warehouse and calculate metrics for time period

Task #3.1. Convert input dataset to parquet. Think about partitioning. Compare performance on top CSV input and parquet input. Save output for Task #1 as parquet as well <br />
Task #3.2. Calculate metrics from Task #2 for different time periods:
* For September 2020
* For 2020-11-11 <br />
Compare performance on top csv input and partitioned parquet input. Print and analyze query plans (logical and physical) for both inputs. <br />

Requirements for Task 3:
* General input dataset should be partitioned by date
* Save query plans as *.MD file. It will be discussed on exam

Build  Weekly purchases Projection within one quarter

### General requirements

The requirements that apply to all tasks:

* Use Spark version 2.4.5 or higher
* The logic should be covered with unit tests
* The output should be saved as PARQUET files
* Configurable input and output for both tasks
* Will be a plus: README file in the project documenting the solution
* Will be a plus: Integrational tests that cover the main method

## Technologies and dependencies
* Scala 2.12.10
* Spark 3.1.1
* sbt 1.4.9

## Requirements
* Git
* Python 3.8 (or higher) and pip3 (package-management system)

## Build instruction
To run project, follow these steps: <br />
1. Open terminal and clone the project from github repository:
```
$ git clone https://github.com/mkrolczykG/python_engineer_to_bigData_engineer_capstone_project1.git
```
```
$ cd <project_cloned_folder>
```
where <project_cloned_folder> is a path to project root directory <br />

2. (Optional) Create and activate virtualenv: <br />
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
5. Start sbt
```
$ sbt run
```
Select task to execute by entering task number from message in terminal. <br />

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

Created by @mkrolczykG - feel free to contact me!

- Slack: Marcin Krolczyk
- E-mail: mkrolczyk@griddynamics.com