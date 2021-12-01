# Task

Using the api response (`api_response.json`) from the New York Times API (specifically Article Search) return news items in a Python dictionary format.

See developer.nytimes.com for the details of the API response. You do not need to requested additional data.


Adhere to the attached template file (data_loader.py), specifically `getDataBatch` should return the results in batches. You can verify the result by running that file with Python 3.

Each result should contain the news results from the NYTimes API as a flattened dictionary. Example for an item (excerpt):

    {
        "web_url": "http://nytimes.com/...",
        "headline.main": "The main headline",
        "headline.kicker": "...",
        ...
    }

In addition to flattening the news results you must enrich each news article result with the data in the file `reference_data.xlsx`. You can use the `Article Id` column in the sheet review_status as the key for matching. Please include the data from both sheets. Please note there may be duplicates, make sure you handle and include this information in each news article result.

Please also provide a requirements.txt file in case your solution depends on any 3rd party library and a README file with any relevant information.
Don’t use a 3rd party library for the dictionary flattening but instead define a function for it as part of the solution.

Background
==========

The template file is a stripped down version of the Squirro data loader
plugins. This is what we use at Squirro to connect to a new data source. While
not required for this task, you may want to look at the data loader
documentation:
https://squirro.atlassian.net/wiki/display/DOC/Data+Loader.
