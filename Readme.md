![Dashboards](./public/Dashboard%20With%20Different%20Pipeline%20Results.PNG)

# Intro
We built a pipeline class that handles data extraction/fetch, transformation, displaying dashboard for n different pipelines and displays them separately on a dashboard.<br>
Currenlty two pipelines are included, we can include as many more as we like.

### Pipelines Included
1. We use Open Meteo's weather data to predict if rain falls on the next day or not.
2. We use Polygon's stock market data to predict the Closing price of a stock for the next day.

### TechStack
1. Plotly Dash
2. Plotly Express, 
3. Mysql
4. Pandas, 
5. LSTM, 
6. XGBClassifier

### Abstraction
1. Datalake
2. Data Warehouse
3. ETL Functions
4. Pipeline workflow.



# How to run:
1. Config:
```
python -m venv .localenv
.localenv\Scripts\python -m pip install -r requirements.txt
```
2. Start the workflow:
```
.localenv\Scripts\python -m src.start newopenmeteo
.localenv\Scripts\python -m src.start newpolygon
```
You can type `new`openmeteo to fetch new data. or `old`openmeteo to get predict using data alreay in the data warehouse.
3. Start the dashboard:
```
.localenv\Scripts\python -m src.start dashboard
```
4. Start multiple at a time:
```
.localenv\Scripts\python -m src.start newopenmeteo newpolygon dashboard
```



# Datasets
Weather data:
https://open-meteo.com/en/docs

Stock market data:
https://polygon.io/




# Environmet Keys Needed
They need to be stored in the `.env` file in:
```
project
    ├─keys
        └─.env
```

```
POLYGON_KEY=
DB_USER=
DB_PASS=
DB_HOST=
```