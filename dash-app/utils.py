import pandas as pd
import numpy as np
import plotly.express as px

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
    
# Filter dataframe by continent (MAP)
def filter_dataframe_by_continent(df, continent):
    filter_df = df

    europe_countries = ['Italy', 'United Kingdom', 'Germany', 'France', 'Netherlands',
                        'Portugal', 'Switzerland', 'Belgium', 'Sweden', 'Denmark',
                        'Poland', 'Russian Federation', 'Austria', 'Greece', 'Norway', 'Finland',
                        'Czech Republic', 'Ireland', 'Slovenia', 'Croatia', 'Serbia',
                        'Estonia', 'Lithuania', 'Cyprus', 'Hungary', 'Slovakia', 'Bulgaria',
                        'Latvia', 'Romania', 'Malta', 'Luxembourg', 'Iceland', 'Belarus',
                        'Ukraine', 'Bosnia and Herzegovina', 'Moldova', 'Albania', 'North Macedonia']
    
    north_america_countries = ['United States', 'Canada', 'Mexico', 'Guatemala', 'Haiti', 'Honduras', 
                               'El Salvador','Nicaragua', 'Costa Rica', 'Panama', 'Cuba', 'Dominican Republic',
                               'Jamaica', 'Puerto Rico', 'Bahamas', 'Greenland', 'Trinidad and Tobago']

    south_america_countries = ['Brazil', 'Chile', 'Argentina', 'Colombia', 'Ecuador',
                               'Peru', 'Venezuela', 'Uruguay', 'Paraguay', 'Bolivia', 'Guyana',
                               'Suriname', 'Falkland Islands (Malvinas)']

    asia_countries = ['China', 'Japan', 'India', 'South Korea', 'Israel', 'Iran',
                      'Turkey', 'Saudi Arabia', 'United Arab Emirates', 'Taiwan',
                      'Pakistan', 'Singapore', 'Hong Kong', 'Thailand', 'Malaysia', 
                      'Viet Nam', 'Lebanon', 'Qatar', 'Bangladesh', 'Armenia', 
                      'Jordan', 'Georgia', 'Philippines', 'Kazakhstan', 'Iraq',
                      'Afghanistan', 'Kyrgyzstan', 'Tajikistan', 'Brunei Darussalam',
                      'Myanmar', 'Laos', 'Indonesia', 'Cambodia', 'Singapore',
                      'Yemen', 'Oman', 'Syrian Arab Republic', 'Azerbaijan', 'Turkmenistan',
                      'Uzbekistan', 'Sri Lanka', 'Mongolia', 'Nepal', 'Bhutan', 'Kuwait',
                      'Cyprus']

    africa_countries = ['South Africa', 'Morocco', 'Egypt', 'Nigeria', 'Algeria',
                        'Tunisia', 'Ethiopia', 'Kenya', 'Ghana', 'Namibia', 'Sudan', 
                        'Libyan Arab Jamahiriya', 'Mauritania', 'Mozambique', 'Zimbabwe',
                        'Mali', 'Angola', 'Gambia', 'Togo', 'Senegal', 'Cameroon', 'Mauritius',
                        'Congo', 'Zambia', 'Uganda', 'Botswana', 'Gabon', 'Rwanda', 'Madagascar',
                        'Niger', 'Malawi', 'Burkina Faso', 'Cape Verde', 'Guinea',
                        'Cote d\'Ivoire', 'Benin', 'Chad', 'Guinea-Bissau', 'Sierra Leone',
                        'Zimbabwe', 'Burundi', 'Liberia', 'Central African Republic', 'Djibouti',
                        'Equatorial Guinea', 'Democratic Republic Congo', 'Tanzania']

    # Apply the masks to the DataFrame and return filtered DataFrames for each continent
    if continent == 'world':
        pass
    elif continent == 'europe':
        filter_df = df[df['country'].isin(europe_countries)]
    elif continent == 'north america':
        filter_df = df[df['country'].isin(north_america_countries)]
    elif continent == 'south america':
        filter_df = df[df['country'].isin(south_america_countries)]
    elif continent == 'asia':
        filter_df = df[df['country'].isin(asia_countries)]
    elif continent == 'africa':
        filter_df = df[df['country'].isin(africa_countries)]

    return filter_df

def determine_text_position(value, scale_mid):
    if value >= scale_mid:
        return 'inside'
    else:
        return 'outside'
    
# Función para dividir y eliminar duplicados
def split_and_remove_duplicates_cities(row):
    affiliation_city = row['affiliation_city']
    if affiliation_city is not None:
        cities = affiliation_city.split(";")
        unique_cities = set(cities)  # Elimina duplicados manteniendo el orden original
        return pd.Series(list(unique_cities), dtype=str)
    else:
        return pd.Series(dtype=str)  # Devuelve una serie vacía si affiliation_city es None

# Función para dividir y eliminar duplicados
def split_and_remove_duplicates_country(row):
    affiliation_country = row['affiliation_country']
    if affiliation_country is not None:
        countries = affiliation_country.split(";")
        unique_countries = set(countries)  # Elimina duplicados manteniendo el orden original
        return pd.Series(list(unique_countries), dtype=str)
    else:
        return pd.Series(dtype=str)  # Devuelve una serie vacía si affiliation_city es None

    
def load_figures(df: pd.DataFrame):

    # ------------ TOP 25 CITIES ------------ #
    # Split the cities separated by ";" into independent rows and get the count of each city
    city_count = df['affiliation_city'].str.split(";").explode('affiliation_city').value_counts()
    # Create a new DataFrame with two columns: 'City' and 'Count'
    df_city_count = pd.DataFrame({'city': city_count.index, 'count': city_count.values})
    # Remove empty rows and select the top 25 cities
    df_city_count = df_city_count[df_city_count['city'] != ''].reset_index(drop=True).head(25).sort_values(by='count', ascending=True)
    # Create a bar chart with the top 25 cities
    fig_cities = px.bar(df_city_count, x='count', y='city', 
                orientation='h', labels={'count': 'Contributions', 'city': 'City'}, 
                title='Top-25 cities', color= 'count', 
                color_continuous_scale=get_color_gradient(c3, c4, 25))

    scale_mid_cities = (max(df_city_count['count']) + min(df_city_count['count'])) / 2
    fig_cities.update_traces(text=df_city_count['city'], textposition=[determine_text_position(value, scale_mid_cities) for value in df_city_count['count']])
    fig_cities.update_layout(yaxis_title='', yaxis_showline=False, yaxis_showticklabels=False, title_x=0.5, coloraxis_showscale=False)

    # ------------ TOP 25 INSTITUTIONS ------------ #
    # Split the cities separated by ";" into independent rows and get the count of each city
    institution_count = df['affilname'].str.split(";").explode('affilname').value_counts()
    # Create a new DataFrame with two columns: 'Institution' and 'num_publications'
    df_institution_count = pd.DataFrame({'institution': institution_count.index, 'num_publications': institution_count.values})
    # Remove empty rows and select 
    df_institution_count = df_institution_count[df_institution_count['institution'] != ''].reset_index(drop=True).head(25).sort_values(by='num_publications', ascending=True)
    # Create a bar chart with the top 25 institutions
    fig_institutions = px.bar(df_institution_count, x='num_publications', y='institution', 
                orientation='h', labels={'num_publications': 'Number of Publications', 'institution': 'Institution'}, 
                title='Top-25 Institutions', color= 'num_publications', 
                color_continuous_scale=get_color_gradient(c1, c2, 25))

    scale_mid_institutions = (max(df_institution_count['num_publications']) + min(df_institution_count['num_publications'])) / 2
    fig_institutions.update_traces(text=df_institution_count['institution'], textposition=[determine_text_position(value, scale_mid_institutions) for value in df_institution_count['num_publications']])
    fig_institutions.update_layout(yaxis_title='', yaxis_showline=False, yaxis_showticklabels=False, title_x=0.5, coloraxis_showscale=False)

    # ------------ TOP 25 FUNDING SPONSORS ------------ #
    # Split the cities separated by ";" into independent rows and get the count of each city
    fund_sponsor_count = df['fund_sponsor'].str.split(";").explode('fund_sponsor').value_counts()
    # Create a new DataFrame with two columns: 'Funding Sponsor' and 'Count'
    df_fund_sponsor_count = pd.DataFrame({'fund_sponsor': fund_sponsor_count.index, 'num_projects': fund_sponsor_count.values})
    # Remove empty rows and select 
    df_fund_sponsor_count = df_fund_sponsor_count[df_fund_sponsor_count['fund_sponsor'] != ''].reset_index(drop=True).head(25).sort_values(by='num_projects', ascending=True)
    # Create a bar chart with the top 25 cities
    fig_fund_sponsor = px.bar(df_fund_sponsor_count, x='num_projects', y='fund_sponsor', 
                orientation='h', 
                labels={'num_projects': 'Number of Projects', 'fund_sponsor': 'Funding Sponsor'}, 
                title='Top-25 Funding Sponsor', color= 'num_projects', 
                color_continuous_scale=get_color_gradient(c1, c2, 25))

    scale_mid_fund = (max(df_fund_sponsor_count['num_projects']) + min(df_fund_sponsor_count['num_projects'])) / 2
    fig_fund_sponsor.update_traces(text=df_fund_sponsor_count['fund_sponsor'], textposition=[determine_text_position(value, 2250) for value in df_institution_count['num_publications']])
    fig_fund_sponsor.update_layout(yaxis_title='', yaxis_showline=False, yaxis_showticklabels=False, title_x=0.5, coloraxis_showscale=False)

    # ------------ OPEN-ACCESS PIE CHART ------------ #
    # Count the number of times each value appears in the 'openaccess' column
    counts = df['openaccess'].value_counts()
    fig_openaccess = px.pie(values=counts, names=counts.index, 
                title= "Distribution of Open Access Projects", 
                color_discrete_sequence=get_color_gradient(c5, c6, 2))
    # Personalize the labels directly in the graph
    fig_openaccess.update_traces(textposition='inside', textinfo='label', textfont_size=13.5, textfont_color='white', labels=['Open Access', 'Subscription'], showlegend=False)
    fig_openaccess.update_layout(title_x=0.5)

    # ------------ CITED-BY COUNT PIE CHART ------------ #
    # Define the ranges
    rangos = [0, 5, 10, 50, df['citedby_count'].max()]
    # Discretize the 'citedby_count' column
    df['citedby_range'] = pd.cut(df['citedby_count'], bins=rangos, right=False)
    # Count how many values fall into each range
    conteo_rangos = df['citedby_range'].value_counts().reset_index()
    # Apply the function to get the labels of the ranges
    conteo_rangos['citedby_range'] = conteo_rangos['citedby_range'].apply(range_label)
    # Create the pie chart with plotly.express
    fig_citedby = px.pie(conteo_rangos, names='citedby_range', values='count',
                        title= "Distribution of Number of Citations",
                        color_discrete_sequence=get_color_gradient(c7, c8, 4))
    # Personalize the labels directly in the graph
    fig_citedby.update_traces(textposition='inside', textinfo='label', textfont_size=13.5, textfont_color='white', showlegend=False)
    fig_citedby.update_layout(title_x=0.5)

    # ---------------------- NUMBER OF PUBLICATIONS PER YEAR ----------------------- #
    # Extract the year from the 'coverDisplayDate' column and create a new 'Year' column
    df['Year'] = df['coverDisplayDate'].str.extract(r'(\d{4})')
    # Create a list of valid years
    years_valid = ['2018', '2019', '2020', '2021', '2022', '2023']
    # Filter the DataFrame to include only the valid years
    df_filtered = df[df['Year'].isin(years_valid)]
    # Calculate the number of publications per year
    publication_counts = df_filtered['Year'].value_counts().reset_index()
    publication_counts.columns = ['Year', 'Number of Publications']
    # Order the values in publication_counts by year
    publication_counts = publication_counts.sort_values(by='Year')
    fig_years = px.line(publication_counts, x='Year', y='Number of Publications', labels={'Número de Publicaciones': 'Número de Publicaciones'},
                title='Number of Publications per Year')
    fig_years.update_layout(title_x=0.5)

    # ---------------------- MAP ----------------------- #
    country_count = df['affiliation_country'].str.split(";").explode('affiliation_country').value_counts()
    df_city_count = pd.DataFrame({'country': country_count.index, 'count': country_count.values})
    df_city_count.drop(df_city_count[df_city_count['country'] == 'Spain'].index, inplace = True)

    # Define the options for the Dropdown menu based on the valid 'scope' values
    continent_options = [
        {'label': 'World', 'value': 'world'},
        {'label': 'Europe', 'value': 'europe'},
        {'label': 'Asia', 'value': 'asia'},
        {'label': 'Africa', 'value': 'africa'},
        {'label': 'North America', 'value': 'north america'},
        {'label': 'South America', 'value': 'south america'}
    ]

    # Create the choropleth map using Plotly Express (keep this outside the layout function)
    fig_map = px.choropleth(df_city_count,
                        locations="country",
                        locationmode='country names',
                        color="count",
                        color_continuous_scale=px.colors.sequential.Emrld,
                        projection='natural earth',  # Default scope: 'world'
                        title="Publications by Country")

    # Adjust the map size
    fig_map.update_layout(
        mapbox=dict(
            center=dict(lat=0, lon=0),  # Adjust the center coordinates of the map
            zoom=1.5,  # Adjust the zoom level
            style="white-bg",  # Set the map style, "white-bg" is a light background
        ),
        height=600,  # Adjust the height of the map
        title_x=0.5,
    )
