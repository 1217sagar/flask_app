# flask_app

1. Install all the required packages
    `pip install Flask pymongo pandas`

2. (Optional) I've inserted the data from csv file into the mongodb database(provided in db_config.py). If you want to redo the process on same db_config or of your choice (change the db_config accordingly) it'll take 2-3 minutes of time.
    `python insert.py`

2. Start Server
    `python index.py`

3. Sample query, open new tab to check api
    `curl --location 'http://127.0.0.1:5000/emissions?startDate=2022-04-18&endDate=2022-04-25&BUSINESS_FACILITY=Fresh%20Kitchen%20Fusionopolis&BUSINESS_FACILITY=Fresh%20Kitchen%20Havelock'`
    -> This query is for two business facilities
    "query": [
            {
                    "key": "startDate",
                    "value": "2022-04-18"
            },
            {
                    "key": "endDate",
                    "value": "2022-08-25"
            },
            {
                    "key": "BUSINESS_FACILITY",
                    "value": "Fresh Kitchen Fusionopolis"
            },
            {
                    "key": "BUSINESS_FACILITY",
                    "value": "Fresh Kitchen Havelock"
            }
    ]
    startDate, endDate and atleast 1 BUSINESS_FACILITY are mandotary.

    -> output : 
        {
          "data": [
            {
              "_id": "Fresh Kitchen Fusionopolis",
              "total_emissions": 18725.336094212
            },
            {
              "_id": "Fresh Kitchen Havelock",
              "total_emissions": 3996.715510701
            }
          ],
          "endDate": "Thu, 25 Aug 2022 00:00:00 GMT",
          "startDate": "Mon, 18 Apr 2022 00:00:00 GMT"
        }

