import pandas as pd
import numpy as np

# Blue color gradient
c1 = [198,219,239]
c2 = [8,81,156]

# Green color gradient
c3 = [199,233,192]
c4 = [0,109,44]

# Blue-Green color gradient
c5 = [102,194,164]
c6 = [65,174,118]

# Yellow color gradient
c7 = [57, 171, 126]
c8 = [237, 239, 93]

# Define the options for the Dropdown menu based on the valid 'scope' values
continent_options = [
    {'label': 'World', 'value': 'world'},
    {'label': 'Europe', 'value': 'europe'},
    {'label': 'Asia', 'value': 'asia'},
    {'label': 'Africa', 'value': 'africa'},
    {'label': 'North America', 'value': 'north america'},
    {'label': 'South America', 'value': 'south america'}
]

# Function extracted from https://medium.com/@BrendanArtley/matplotlib-color-gradients-21374910584b
def get_color_gradient(c1, c2, n):
    """
    Given two rgb colors, returns a color gradient
    with n colors.
    """
    assert n > 1
    c1_rgb = np.array(c1)/255
    c2_rgb = np.array(c2)/255
    mix_pcts = [x/(n-1) for x in range(n)]
    rgb_colors = [((1-mix)*c1_rgb + (mix*c2_rgb)) for mix in mix_pcts]
    return ["#" + "".join([format(int(round(val*255)), "02x") for val in item]) for item in rgb_colors]

# Function to get the label of the range
def range_label(range):
    if range.left == 0 and range.right == 5:
        return '< 5'
    elif range.left == 5 and range.right == 10:
        return '5 - 9'
    elif range.left == 10 and range.right == 25:
        return '10 - 24'
    else:
        return '25 >'

def determine_text_position(value, scale_mid):
    if value >= scale_mid:
        return 'inside'
    else:
        return 'outside'
    
# Function to divide and remove duplicates.
# This is required for counting the number of cities and countries that can appear more than one time 
def split_and_remove_duplicates_cities(row):
    affiliation_city = row['affiliation_city']
    if affiliation_city is not None:
        if not isinstance(affiliation_city, str):
            affiliation_city = str(affiliation_city)
        cities = affiliation_city.split(";")
        unique_cities = set(cities) 
        return pd.Series(list(unique_cities), dtype=str)
    else:
        # Returns an empty series if affiliation_city is None
        return pd.Series(dtype=str)

def split_and_remove_duplicates_country(row):
    affiliation_country = row['affiliation_country']
    if affiliation_country is not None:
        if not isinstance(affiliation_country, str):
            affiliation_country = str(affiliation_country)        
        countries = affiliation_country.split(";")
        unique_countries = set(countries) 
        return pd.Series(list(unique_countries), dtype=str)
    else:
        # Returns an empty series if affiliation_country is None
        return pd.Series(dtype=str)  

