#Import dependencies
import json
import pandas as pd
import numpy as np
import re
import time
# database connection
from sqlalchemy import create_engine
import psycopg2
import sys
sys.path.append("..")
from config import db_password

#Function to clean alternate movie titles into separate column
alt_titles_list = ['Also known as', 'Arabic', 'Cantonese', 'Chinese', 'Hangul', 'Hepburn', 'Japanese', 
             'McCune–Reischauer', 'Mandarin', 'Original title', 'Polish', 'Romanized', 
                   'Revised Romanization', 'Russian', 'Simplified', 'Traditional', 'Yiddish', 
                   'French', 'Hebrew']
def clean_movie(movie):
    # copy movie entry to not destroy original as local variable
    movie = dict(movie)
    # empty dictionary to hold each alt title in movie
    alt_titles = {}
    # loop through values in alt_titles_list to find match for key in dictionary movie
    for key in alt_titles_list:
        if key in movie:
            # set key value of new dictionary to value in movie dictionary
            alt_titles[key] = movie[key]
            # remove key from movie
            movie.pop(key)
    # add concise dictionary to movies as 1 key
    if len(alt_titles) > 0:
        movie['alt_titles'] = alt_titles
        
    # function to unify key names in dictionaries corresponding to same values
    def change_column_name(old_name, new_name):
        if old_name in movie:
            movie[new_name] = movie.pop(old_name)

    
    # call column change function within clean_movie function before return
    change_column_name('Adaptation by', 'Writer(s)')
    change_column_name('Country of origin', 'Country')
    change_column_name('Directed by', 'Director')
    change_column_name('Distributed by', 'Distributor')
    change_column_name('Edited by', 'Editor(s)')
    change_column_name('Length', 'Running time')
    change_column_name('Original release', 'Release date')
    change_column_name('Music by', 'Composer(s)')
    change_column_name('Produced by', 'Producer(s)')
    change_column_name('Producer', 'Producer(s)')
    change_column_name('Productioncompanies ', 'Production company(s)')
    change_column_name('Productioncompany ', 'Production company(s)')
    change_column_name('Released', 'Release Date')
    change_column_name('Release Date', 'Release date')
    change_column_name('Screen story by', 'Writer(s)')
    change_column_name('Screenplay by', 'Writer(s)')
    change_column_name('Story by', 'Writer(s)')
    change_column_name('Theme music composer', 'Composer(s)')
    change_column_name('Written by', 'Writer(s)')
    

    return movie

# Function to import data files
def extract_transform_load():
    
    kaggle_metadata = pd.read_csv(kaggle_file)
    ratings = pd.read_csv(ratings_file)

    with open(wiki_file, mode='r') as file:
        wiki_raw = json.load(file)
    wiki_movies = [movie for movie in wiki_raw if 'No. of episodes' not in movie]

    # and call the clean_movie function on each movie.
    cleaned_movies = [clean_movie(movie) for movie in wiki_movies]
    wiki_movies_df = pd.DataFrame(cleaned_movies)

    try:
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)

    except:
        print(f"Regex error: {wiki_movies_df['imdb_link']} for {wiki_movies_df['title']}")

    # List comprehension to keep the columns that don't have null values from the wiki_movies_df DataFrame.
    cols_to_keep = [cols for cols in wiki_movies_df.columns if wiki_movies_df[cols].isnull().sum() < len(wiki_movies_df)*0.9]
    wiki_movies_df = wiki_movies_df[cols_to_keep]

    # box_office data clean
    box_office = wiki_movies_df['Box office'].dropna()
    box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)

    # Regular expression to match box office data
    form_one = r'\$\s?\d+\.?\d*\s+[mb]illi?on'
    form_two = r'\$\s?\d{1,3}(?:[,\.]+\d{3}){1,4}(?!\s[mb]illion)'

    # Function to parse dollar amounts 
    def parse_dollars(s):
    # return NaN for non-string values
        if type(s) != str:
            return np.nan
        # Matching $#.### million
        if re.match(r'\$\s?\d+\.?\d*\s+milli?on', s, flags=re.IGNORECASE):
            # remove $, space, wording
            s = re.sub(r'\$|\s|[a-zA-Z]', '', s)
            # convert data type and multiply to get number
            value = float(s) * 10**6
            # return value
            return value
        # Matching $#.### billion
        elif re.match(r'\$\s?\d+\.?\d*\s+billion', s, flags=re.IGNORECASE):
            # remove $, space, wording
            s = re.sub(r'\$|\s|[a-zA-Z]', '', s)
            # convert data type and multiply to get number
            value = float(s) * 10**9
            # return value
            return value
        #Matching $#,###
        elif re.match(r'\$\s?(?:\d{1,3}[,\.]+){1,4}(?!\s[mb]illion)', s, flags=re.IGNORECASE):
            # remove $, space, commas
            s = re.sub(r'\$|\s|,', '', s)
            # convert to float
            value = float(s)
            # return value
            return value
        # For non-matching values
        else:
            return np.nan
         
    # Call parse dollars function on box-office data
    box_office = box_office.str.replace(r'\$.*[-–—](?![a-zA-Z])', '$', regex=True)
    wiki_movies_df['box_office'] = box_office.str.extract(f"({form_one}|{form_two})", flags=re.IGNORECASE)[0].apply(parse_dollars)
    
    # Clean the budget column in the wiki_movies_df DataFrame.
    budget = wiki_movies_df['Budget'].dropna()
    budget = budget.apply(lambda x: ' '.join(x) if type(x) == list else x)
    budget = budget.str.replace(r'\$.*[-–—](?![a-zA-Z])', '$', regex=True)
    budget = budget.str.replace(r'\[\d+\]\s*', '', regex=True)
    wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)


    # Clean the release dates
    release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    # matching Month DD, YYYY
    date_form_one = r'(?:January|Febuary|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4}'
    # matching YYYY-MM-DD
    date_form_two = r'\d{4}\W[0-1][0-2]\W[0-3][0-9]'
    # matching Month YYYY
    date_form_three = r'(?:January|Febuary|March|April|May|June|July|August|September|October|November|December)\s+\d{4}'
    # matching YYYY
    date_form_four = r'\d{4}'
    wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})', flags=re.IGNORECASE)[0], infer_datetime_format=True)

    # Clean the running time data
    running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
    running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|^(\d*)\s*m', flags=re.IGNORECASE).apply(lambda x: pd.to_numeric(x, errors='coerce')).fillna(0)
    wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0] * 60 + row[1] if row[2] == 0 else row[2], axis=1)
    wiki_movies_df.drop(["Box office", "Budget", "Running time", "Release date"], axis=1, inplace=True)
    
    # Clean the Kaggle metadata.
    kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == "False"].drop('adult', axis='columns')
    kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
    kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
    kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
    kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])

    # Merged the two DataFrames
    movies_df = wiki_movies_df.merge(kaggle_metadata, on='imdb_id', suffixes=['_wiki', '_kaggle'])
    movies_df.drop(columns=['release_date_wiki', 'title_wiki', 'Language', 'Production company(s)', 'video'], inplace=True)
    
    # function to fill in the missing Kaggle data.
    def fill_missing_kaggle(wiki_data, kaggle_data):
        # function to sub 0 values in kaggle data with wiki data
        movies_df[kaggle_data] = movies_df.apply(lambda row: row[wiki_data] 
                                                 if row[kaggle_data] == 0 
                                                 else row[kaggle_data], axis=1)
        # drop wiki rows
        movies_df.drop(columns=wiki_data, inplace=True)

    # call function with column-pair parameters
    fill_missing_kaggle('budget_wiki', 'budget_kaggle')
    fill_missing_kaggle('box_office', 'revenue')
    fill_missing_kaggle('running_time', 'runtime')

    # Filter the movies DataFrame columns.
    movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                       'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                       'genres','original_language','overview','spoken_languages','Country',
                       'production_companies','production_countries','Distributor',
                       'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                      ]]

    # Rename the columns in the movies DataFrame.
    movies_df.rename({'id':'kaggle_id',
                  'title_kaggle':'title',
                  'url':'wikipedia_url',
                  'budget_kaggle':'budget',
                  'release_date_kaggle':'release_date',
                  'Country':'country',
                  'Distributor':'distributor',
                  'Producer(s)':'producers',
                  'Director':'director',
                  'Starring':'starring',
                  'Cinematography':'cinematography',
                  'Editor(s)':'editors',
                  'Writer(s)':'writers',
                  'Composer(s)':'composers',
                  'Based on':'based_on'
                 }, axis='columns', inplace=True)

    # Transform and merge the ratings DataFrame.
    ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
    ratings_count = ratings.groupby(['movieId', 'rating'], as_index=False).count() \
                .rename({'userId': 'count'}, axis=1) \
                .pivot(index='movieId', columns='rating', values='count')
    ratings_count.columns = ['rating_' + str(col) for col in ratings_count.columns]
    movies_with_ratings_df = movies_df.merge(ratings_count, left_on='kaggle_id', right_index=True, how='left')
    movies_with_ratings_df[ratings_count.columns] = movies_with_ratings_df[ratings_count.columns].fillna(0)

    #Load data
    # connection string
    db_string = f"postgresql://postgres:{db_password}@localhost:5432/movie_data"
    # database engine
    engine = create_engine(db_string)
    # load movies df to sql as movies table
    movies_df.to_sql(name='movies', con=engine, if_exists='replace')
    #load ratings data - for large data sizes
    rows_imported = 0
    start_time = time.time()
    for data in pd.read_csv(f'{file_dir}/ratings.csv', chunksize=1000000):
        # print rows importing status
        print(f"Rows importing: {rows_imported} to {rows_imported + len(data)}...", end='')
        # append section to db 
        data.to_sql(name='ratings', con=engine, if_exists='append')
        # update rows_imported
        rows_imported += len(data)
        # print done when finished with each section loop
        print(f"Done. {time.time()-start_time} elapsed time.")

# pathways to file
file_dir = "Resources"
wiki_file = f'{file_dir}/wikipedia-movies.json'
kaggle_file = f'{file_dir}/movies_metadata.csv'
ratings_file = f'{file_dir}/ratings.csv'

# Function call and variable store
wiki_file, kaggle_file, ratings_file = extract_transform_load()
