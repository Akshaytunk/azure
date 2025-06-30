from langchain_openai import AzureChatOpenAI
import streamlit as st
import pandas as pd
from langchain.chains import RetrievalQA
from langchain.prompts import PromptTemplate
from langchain_core.prompts import PromptTemplate,FewShotPromptTemplate
from langchain.schema.output_parser import StrOutputParser
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent 
from langchain.agents import AgentExecutor 
from langchain.agents.agent_types import AgentType
import pyodbc
from sqlalchemy import create_engine
import os
import json



def sql_query(question,llm):
    # loading Azure sql database
    # Azure SQL Database connection details
    server = 'teksqldbserver'
    database = 'TekSQLDB'
    username = 'sqldbadmin'
    password = 'Tek@1234'
    driver = '{ODBC Driver 18 for SQL Server}'

    #connection_string = f'mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+18+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no&Connection Timeout=60'
    connection_string = 'mssql+pyodbc:///?odbc_connect=' 'Driver='+driver+ ';Server=tcp:' + server+'.database.windows.net;PORT=1433' + ';DATABASE='+database + ';Uid=' +username+ ';Pwd=' + password + ';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
    engine = create_engine(connection_string)
    db = SQLDatabase(engine,view_support=True,schema="Margin")


    few_shot_examples = [
        {
        
        }
        ]
    # Create a metadata about database in json
    metadata=  {   "schema" :"Margin",
                    "tables":[{"name":"FactData",
                            "description":"stores transactional data",
                            "columns":[
                                {"name":"Version","type":"string","description":"Version of data"},
                                {"name":"Date","integer":"Integer","description":"Date of transaction"},
                                {"name":"Product_Model_Code","type":"string","description":"Unique ID for each Product"},
                                {"name":"Account","type":"string","description":"Column containing metrics"},
                                {"name":"Channel","type":"string","description":"Column containing channel name"},
                                {"name":"SignedData","type":"float","description":"Column containing numerical values for metrics"}
                                        ]
                            },
                            
                            {"name":"Product_MasterData",
                                "description":"Stores information Product",
                                "columns":[
                                        {"name":"Product_Model_Code","type":"string","description":"Unique ID for each Product"},
                                        {"name":"Product_Model_Description","type":"string","description":"Description of Product"},
                                        {"name":"Brand","type":"string","description":"Brand of Product"},
                                        {"name":"Carline","type":"string","description":"Carline of Product"},
                                        {"name":"Model_Year","type":"Integer","description":"Manufacture Year of Product"}
                            
                                        ]
                            },
                            {"name":"Options_COST",
                                "description":"Stores information of Option Codes for cost account",
                                "columns":[
                                        {"name":"Version","type":"string","description":"Version of data"},
                                        {"name":"Date","integer":"Integer","description":"Date of transaction"},
                                        {"name":"Product_Model_Code","type":"string","description":"Unique ID for each Product"},
                                        {"name":"Options","type":"string","description":"Column containing option code for Product"},
                                        {"name":"Account","type":"string","description":"Column containing metrics"},
                                        {"name":"Channel","type":"string","description":"Column containing channel name"},
                                        {"name":"SignedData","type":"float","description":"Column containing numerical values for metrics"}
                            
                                        ]

                            },
                            {"name":"Options_REV",
                                "description":"Stores information of Option Codes for revenue account",
                                "columns":[
                                        {"name":"Version","type":"string","description":"Version of data"},
                                        {"name":"Date","integer":"Integer","description":"Date of transaction"},
                                        {"name":"Product_Model_Code","type":"string","description":"Unique ID for each Product"},
                                        {"name":"Options","type":"string","description":"Column containing option code for Product"},
                                        {"name":"Account","type":"string","description":"Column containing metrics"},
                                        {"name":"Channel","type":"string","description":"Column containing channel name"},
                                        {"name":"SignedData","type":"float","description":"Column containing numerical values for metrics"}
                            
                                        ]

                            },
                            {"name":"EC_COST",
                                "description":"Stores information of Exterior color Codes for cost account",
                                "columns":[
                                        {"name":"Version","type":"string","description":"Version of data"},
                                        {"name":"Date","integer":"Integer","description":"Date of transaction"},
                                        {"name":"Product_Model_Code","type":"string","description":"Unique ID for each Product"},
                                        {"name":"Exterior_Color","type":"string","description":"Column containing exterior color code for Product"},
                                        {"name":"Account","type":"string","description":"Column containing metrics"},
                                        {"name":"Channel","type":"string","description":"Column containing channel name"},
                                        {"name":"SignedData","type":"float","description":"Column containing numerical values for metrics"}
                            
                                        ]

                            },
                            {"name":"EC_REV",
                                "description":"Stores information of Exterior color Codes for revenue account",
                                "columns":[
                                        {"name":"Version","type":"string","description":"Version of data"},
                                        {"name":"Date","integer":"Integer","description":"Date of transaction"},
                                        {"name":"Product_Model_Code","type":"string","description":"Unique ID for each Product"},
                                        {"name":"Exterior_Color","type":"string","description":"Column containing exterior color code for Product"},
                                        {"name":"Account","type":"string","description":"Column containing metrics"},
                                        {"name":"Channel","type":"string","description":"Column containing channel name"},
                                        {"name":"SignedData","type":"float","description":"Column containing numerical values for metrics"}
                            
                                        ]

                            }

                            ],
                    "relationships":[

                        {
                            "table1":"FactData",
                            "table2":"Product_MasterData",
                            "key1":"Product_Model_Code",
                            "key2":"Product_Model_Code",
                            "description":"Links FactData to Product_MasterData via Product_Model_Code"
                        },
                        {
                            "table1":"Options_COST",
                            "table2":"Product_MasterData",
                            "key1":"Product_Model_Code",
                            "key2":"Product_Model_Code",
                            "description":"Links Options_COST to Product_MasterData via Product_Model_Code"
                        },
                        {
                            "table1":"Options_REV",
                            "table2":"Product_MasterData",
                            "key1":"Product_Model_Code",
                            "key2":"Product_Model_Code",
                            "description":"Links Options_REV to Product_MasterData via Product_Model_Code"
                        },
                        {
                            "table1":"EC_COST",
                            "table2":"Product_MasterData",
                            "key1":"Product_Model_Code",
                            "key2":"Product_Model_Code",
                            "description":"Links EC_COST to Product_MasterData via Product_Model_Code"
                        },
                        {
                            "table1":"EC_REV",
                            "table2":"Product_MasterData",
                            "key1":"Product_Model_Code",
                            "key2":"Product_Model_Code",
                            "description":"Links EC_REV to Product_MasterData via Product_Model_Code"
                        }

                                ]
                   
    }

    metadata_json=json.dumps(metadata,indent=4)

    # Create a few-shot prompt template
    example_prompt = PromptTemplate(
        input_variables=["question","metadata_json"],
        template=f'''
### SQL Expert: Aggregated Data Interpretation & Detailed Output
You are a SQL expert responsible for interpreting user queries and providing aggregated data outputs in json.Follow these strict rules:  

### 1. Query Interpretation & Aggregation:
- Always infer aggregation when a user provides a column name without specifying an aggregation function.  
- Default aggregation function: SUM (unless the user specifies otherwise).  
- If aggregation is performed on a subset of columns, always include a GROUP BY clause.  
- Reference tables using the format **schema.table** to ensure proper SQL query structure. 
- Use TOP keyword instead of LIMIT 
- Refer tables using the format schema.table to ensure proper SQL query structure.
.

### 2. The data is in long format, where different metric types are represented in the 'Account' column.
The actual numerical values are stored in the 'SignedData' column.

### 3. When user asks for any metric like 'Total Gross Margin' , 'Total Revenue' apply a WHERE filter on that column (e.g., Account = 'Total Gross Margin')
and after filtering by the metric, apply the relevant aggregation function (e.g., SUM, AVG) on the 'SignedData' column, depending on the user query.

### 4. The 'Date' column contains the date in 'yyyymm' format (e.g., 202501 for Jan 2025). The data is of only 2025 year.

### 4.1 A.📘 VERSION-AWARE REPORTING LOGIC
 
You must always handle the `Version` column explicitly. Never aggregate or summarize across versions.
 
Follow these strict rules:
 
---
 
### B. 🚫 NO AGGREGATION ACROSS VERSIONS
 
- You must never return results where multiple versions are aggregated together.
- Always include `Version` in the `WHERE`, `SELECT`, or `GROUP BY` clauses.
- If filtering by `Date` or other fields, keep each version’s data isolated.
 
---
 
### C. ✅ SINGLE VERSION REQUEST
 
If the user explicitly asks for a single version, such as:
 
> “Show Retail Volume for March in version V1”
 
Then:
- Filter using:
  ```sql
  WHERE Version = 'V1'
 
Do not group by Version.
 
Return data only for the specified version.
 
### D. 🔄 MULTI-VERSION COMPARISON
If the user asks to compare versions, such as:
 
“Compare Total Gross Margin for versions Actual and V1”
 
Then:
 
Filter using:
WHERE Version IN ('Actual', 'V1')
 
Include Version in the SELECT and GROUP BY clauses.
 
Return one row per group (e.g., Carline or other entity/entities asked by user + Version).
 
### E. VERY IMPORTANT: VERSION NOT SPECIFIED
If the user does not mention a version:
Do not return the data.
Respond with a clarifying prompt such as:
“Please specify the version (e.g., Actual, V1) you want to analyze. Aggregation across versions is not allowed.”
Only proceed after the user provides a version.

### 5. The 'Model_Year' columns contain numeric values but should be treated as categorical (not aggregated or used in math).

### 6. Important Matching Logic:
- Metric names mentioned by users may differ slightly in wording, spacing, or punctuation. For example, a user may say "retail price" while the actual value in the column is "retail_price".
- First check for metric in 'Account' . Use **fuzzy matching** or **semantic similarity** to infer the intended metric name if an exact match of the metric is not found.

### 7. - Avoid assuming that the metric is a column. Instead, search for the metric name inside known metric-related columns.
### 8. Metadata-Driven Query Construction  
- **Before generating SQL, refer to the provided database metadata:{metadata_json}  
- ALWAYS follow the relationships in metadata to determine the correct JOIN conditions.
- The information may contain in one or more tables mentioned in metadata.Search for relevant information in all tables mentioned in metadata.
- Refer to tables which are mentioned in metadata before generating SQL query. Only the tables included in the metadata contains relevant information.
### 9. When 'variance' is mentioned in user input , consider it as 'difference' and calculate the difference.
Important : Unless user asks for information at Carline level do not aggregate metrics at Carline level.

### 10. "EC_COST" table has only one metric "Exterior Color Factory Transfer Price". Refer to "EC_COST" table only when user asks for metrics like ["Exterior Color Factory Transfer Price","Exterior Color Cost","EC Cost"]

### 11. "OPTIONS_COST" table has only one metric "Options Factory Transfer Price" . Refer to "OPTIONS_COST" table only when user asks for metrics like ["Options  Factory Transfer Price","Options Cost"]

--------------

### 12. "EC_REV" table has only two metrics "Exterior Color Dealer Margin" and "Exterior Color MSRP". When user asks about "Exterior Color Revenue" consider following calculation
Calculation:
"Exterior Color Revenue" = "Exterior Color MSRP" - "Exterior Color Dealer Margin"

### 13. "OPTIONS_REV" table has only two metrics "Options Dealer Margin" and "Options MSRP". When user asks about "Options Revenue" consider following calculation
Calculation:
"Options Revenue" = "Options MSRP" - "Options Dealer Margin"

### 14. 📘 METRIC NAME RESOLUTION RULES

You are allowed to use **close variants or abbreviations**, but only if they are listed in the alias dictionary below.

Metric Alias Dictionary:
{{
  "Total Revenue Schedule 1": ["Total Revenue S1", "Revenue Schedule 1", "Revenue S1"],
  "Total Cost of Sales Schedule 1": ["Total Cost of Sales Schedule 1", "Cost S1", "Cost Schedule 1"],
  "Total Gross Margin": ["Gross Margin", "TGM"],
  "Total Revenue": ["Revenue"],
  "Total Cost of Sales": ["Cost of Sales"],
  "Exterior Color Factory Transfer Price" : ["Exterior Color Factory Transfer Price","Exterior Color Cost","EC Cost"],
  "Options  Factory Transfer Price" : ["Options  Factory Transfer Price","Options Cost"]
}}

Rules:
14.1. If the user uses an alias (e.g., "Total Revenue S1"), map it to the canonical metric (e.g., "Total Revenue Schedule 1") using the dictionary above.
14.2. All SQL filters on metric names (e.g., `WHERE Account = ...`) should use the **canonical metric name**, not the alias.

   ✅ Example:
   If user asks: "Why is Swift top in Total Revenue S1 in 2025?"
   → Use: `Account = 'Total Revenue Schedule 1'`
14.3. When User asks about metric "Total Cost of Sales Schedule 1" consider the metric "Total Cost of Sales Schedule 1" not "Total Cost of Sales".
   When User asks about metric "Total Revenue Schedule 1" consider the metric "Total Revenue Schedule 1" not "Total Revenue".
   This allows for abbreviation flexibility while preserving metric integrity.

***VERY IMPORTANT***
### The metrics ["Exterior Color Factory Transfer Price","Options Factory Transfer Price","Options Cost","Total Cost of Sales Schedule 1"] are cost related then:
   - when user asks question like " which option has highest option cost or any thing based on cost related metrics.
   - Use `ORDER BY <metric_column> ASC` instead of `DESC` for top n highest of these cost related metrics.
   
### 15. Metric Dependency Dictionary: You are provided with a dictionary that defines metric dependencies. Each key is a primary metric, and the corresponding value is a list of dependent metrics. Some dependent metrics come from specific tables.

{{
  "Total Gross Margin": ["Total Revenue", "Total Cost of Sales", "License Fee accrual"],
  "Total Revenue Schedule 1": ["vehicle revenue", "Deferred revenue", "Care MDO"],
  "Total Cost of Sales Schedule 1": [
    "Factory Transfer Price" from "FactData" table,
    "Options Factory Transfer Price" from "Options_COST" table,
    "Exterior Color Factory Transfer Price" from "EC_COST" table
  ]
}}

Task:
When the user asks a question which includes 'why' like, CONSIDER FOLLOWING STEPS ONLY WHEN "Why" is included in user question:  
**"Why does [Carline] have the highest [Metric] during [Time Period]?"**, follow these steps:

15.1. Identify the **main metric** mentioned in the question (e.g., Total Gross Margin).
Identify the main metric:
- Parse the user's question to detect the main metric (e.g., "Total Cost of Sales Schedule 1").
- Use the Metric Dependency Dictionary to find the **list of dependent metrics** and the **source table** for each (if specified).
- If no table is specified, assume all metrics come from a default table (e.g., `FactData`).


15.2. Use the dictionary to find the **dependent metrics** for that main metric.
15.3. Filter the data for the **given period**, which could be a year (e.g., "2025") or a full month (e.g., "202501"):
   - If it's a year (YYYY): filter rows for that year
   - If it's a specific month (YYYYMM): filter rows for that month

15.4. Present the result in a table with the following columns for the top5 carlines based on the main metric. Use TOP5 carline instead filtering data for only one carline :
   - Carline
   - Main Metric 
   - Dependent Metric 1
   - Dependent Metric 2
   - (and so on...)
   -### IMPORTANT: After showing the table, explain **why** the specified carline had the highest value.
   - Base your explanation on which dependent metrics were significantly higher.
   - Do not just restate the fact that the carline was highest — instead, compare the actual contributors (e.g., Total Gross Margin or Total Cost of Sales Schedule 1 etc..) with other carlines.

15.5.5 VERY IMPORTANT AND DO NOT OVERLOOK: Do not use sum(case when...) when you are joining more than two tables with Product_MasterData table.
15.5.6 Do not forget to include the dependent metrics in the output table when user includes 'why' in question like **why** the specified carline had the highest value.
15.5.7 Consider the following steps of CTE to generate output table containing main metric and dependent metric for carline:
step 1 : Get the list of Main metric and the dependent metrics along with the table of metrics.
step 2 : Calculate aggregation of main metric in first common table of expression(CTE) after joining the "FactData" table with "Product_Masterdata" table and group by Carline.
step 3 : Calculate aggregation of dependent metric in second common table of expression(CTE) after joining the table which contains the dependent metric table with "Product_Masterdata" table, group by Carline and apply filter for date. Include the dependent metrics in the query if the dependent metrics are in "FactData" table.
step 3 : Calculate aggregation of one more dependent metric in second common table of expression(CTE) after joining the table which contains the dependent metric table with "Product_Masterdata" table , group by Carline and apply filter for date. Include the dependent metrics in the query if the dependent metrics are in "FactData" table.
step 4 : so on ..... to similarly calculate aggregation of other dependent metrics in other common table of expression(CTE) after joining the table which contains the dependent metric table with "Product_Masterdata" table , group by Carline and apply filter for date.
 Do not miss any dependent metrics , include all the dependent metrics with one CTE for each dependent metric.The number of CTE should be the number of main metric and dependent metrics.
step 5 : Generate Final Query: Join all pre-aggregated data by Carline
step 6 : Generate table containing main metrics and dependent metrics of top 5 carlines based on the main metric.
For example : If user asks why specific carline has highest total cost of sales schedule 1 during certain period.
The output table should contain carline,"Total cost of sales schedule 1", "Factory Transfer Price","Options Factory Transfer Price","Exterior Color Factory Transfer Price" of top 5 carlines based on total cost of  sales schedule 1 during the specified period.
15.5 ❗ IMPORTANT: Grouping for Carline-Based Questions
When the user's question is about carline (e.g., “Which carline has the highest X?” or “Why did [Carline] have highest Y?”):
15.5.1. For every metric table (like FactData, EC_COST, Options_COST), you **must join it with `Product_MasterData` first** using `Product_Model_Code` to retrieve the `Carline`.
15.5.2. Perform all aggregations **grouped by `Carline`**, not by `Product_Model_Code`.

   ✅ Correct:
   ```sql
   JOIN Product_MasterData ON table.Product_Model_Code = PM.Product_Model_Code
   GROUP BY PM.Carline
   ```
15.5.3. However, **DO NOT join any other tables (e.g., Options_COST, EC_COST)** with `FactData` before aggregation.
   - These tables must be **aggregated independently first** (in separate CTEs or subqueries).
   - Then you can safely **join the aggregated results** by `Carline` or `Product_Model_Code`.

   ❌ Incorrect: JOIN Options_COST → then SUM
   ✅ Correct: SUM from Options_COST → then JOIN result

This prevents row multiplication due to one-to-many joins and ensures accurate aggregations.

15.5.4 VERY IMPORTANT:
🚫 DO NOT DERIVE the main metric from dependent metrics

- The main metric is NOT calculated from the dependent metrics. It must be **retrieved directly** from the data (e.g., using `Account = '<Main Metric>'`).
- Dependent metrics are used ONLY to provide context — not to calculate the main metric.
- ❌ Never write: `MainMetric = Metric1 + Metric2 + Metric3`
- ✅ Instead: query `SUM(SignedData)` where `Account = 'Main Metric'`

🔒 Block NULL carlines

- Always exclude records where `Carline IS NULL`.
- Ensure that `Carline` is fetched by joining `Product_MasterData` early in each CTE or subquery.
- Add `WHERE Carline IS NOT NULL` to each aggregated CTE or the final result query.


*** VERY IMPORTANT ***
### 16. when user ask question like "what is the gross margin variance for 202503 between versions V2 and V3"
- present result in a table with volume for each carline and versions V2 and V3
- Please compare Gross Margin variance for 202503 between V2 and V3, 
considering volume, 
include volume data for each carline in V2 and V3, 
and why is there high variance between these versions, 
focusing on volume impact?

16.1. 📈 GROSS MARGIN VARIANCE — VOLUME IMPACT LOGIC:
When user asks about Gross Margin variance between two versions (e.g., V2 and V3):

Include Total Gross Margin AND Volume per carline, per version.

Create a variance column for Gross Margin and another for Volume.

In the explanation, always check if there is a significant difference in Volume between the versions.

If Volume in V3 is lower than V2 → Explain that lower actual volumes than forecasted can reduce realized gross margin.

If Option sales didn’t occur as expected → Mention that lower option uptake impacts total margin.

Sample logic:

“Gross Margin variance of 140,000 is largely driven by a volume difference of 12,000 units between V2 and V3.”

“Forecasted volumes in V3 were higher, and the actuals in V2 were significantly lower, leading to margin drop.”

❗ Emphasize that the Gross Margin difference is not only price-related but is often volume-driven.

*** IMPORTANT ***
### 17. Navigation Links : Give the user the navigation links when ever user asks queries on the below: (example: If the user ask query as : What is gross margin variance for 202503 between V2 and V3 ?)
- When discussing Gross Margin, always include this note:
"If you want to check Gross Margin details, please navigate to the [Gross Margin Dashboard](https://your-gross-margin-link.com)"
- When discussing Options cost or options revenue, always include this note:
"If you want to check Options Level details, please navigate to the [Options Dashboard](https://your-Options-link.com)"
- When discussing Exterior Color cost or Exterior Color revenue, always include this note:
"If you want to check Exterior Color Level details, please navigate to the [Exterior Color Dashboard](https://your-Exterior Color-link.com)"



By following these rules, generate a fully detailed natural language output that accurately represents the SQL query result, ensuring it contains all necessary numerical data.  

        user input : {question}

        '''
    )

    few_shot_prompt = FewShotPromptTemplate(
        examples=few_shot_examples,
        example_prompt=example_prompt,
        suffix="Input: {input}\nSQL Query:"
    )

    # ------setup agent

    # define agent toolkit
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    # define agent executor with agent type ZERO_SHOT_REACT_DESCRIPTION
    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True,
        agent_type="tool-calling",
        handle_parsing_errors=True,
        return_intermediate_steps=True
    )



        
    #prompt = few_shot_prompt.format(input=question)
    res = agent_executor.invoke(few_shot_prompt)

    #if "gross margin" in question.lower() or "gross margin" in output.lower():
        #output += "\n\nIf you want to check Gross Margin details, please navigate to the [Gross Margin Dashboard](https://your-gross-margin-link.com)"
    
    #if "revenue" in question.lower() or "revenue" in output.lower():
        #output += "\n\nFor detailed Revenue analysis, visit our [Revenue Insights Portal](https://your-revenue-link.com)"

    
    print(res["output"])
    return res["output"]



st.header("TEK GenAI")
query = st.text_input("Enter query")
 # Initialize the LLM
    # Defining llm
    
llm = AzureChatOpenAI(
    azure_deployment="gpt-4o-2",  # your model deployment name on Azure OpenAI
    azure_endpoint= os.getenv("open_ai_endpoint"),  # Azure endpoint
    openai_api_version="2023-05-15",  # Azure-specific API version
    openai_api_key=os.getenv("open_api_key"),  # Use environment variables to store this securely
    model="gpt-4o-2",
    temperature=0  # You can adjust this for more or less randomness
    
    )
if query:
    res = sql_query(query,llm)
    st.write(res)
       
    


    


    
