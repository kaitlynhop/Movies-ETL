# Movies-ETL
## Overview
An ETL pipeline was created, using sourced movie data from kaggle and wikipedia. Data was then transformed in Jupyter Notebook using exploratory analysis. Transformation techniques utilized:
1. Columns were consolidated based on data points
2. Unnecessary, redundant, inappropriate, incomplete data points removed 
3. Data types were converted using RegEx and PANDAS built-in functions: to_datetime(), to_numeric()
4. DataFrames were merged on movie ID to singularly show movies with ratings

The transformed data was then loaded into postgeSQL database using SQLAlchemy library

### Purpose
The purpose of this analysis was to create an ETL pipeline, under 1 function that extracts, transforms, and loads data into database. 

### Resources
**Data retrieval:** [Resources](/Resources/)
**Exploratory Analysis:** [Exploratory Analysis](/Exploratory_analysis/)
**Final Function:** [ETL Function](/ETL_pipeline.py)

**Tools:** Python, Jupyter Notebook, PANDAS, PostgreSQL, pgAdmin, RegEx, SQLAlchemy, VSCode
<br>

## Results
<br>**Image 1. Rows Imported**
<br>![Image link](/Resources/Ratings_load.png)
<br>
<br>Image 1. Over 26,000,000 rows of data were imported into database.
<br>
