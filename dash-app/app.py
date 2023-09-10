# Import packages
from dash import Dash, html, dcc
import pandas as pd
import numpy as np
import logging
import requests
import plotly.express as px
from dash.dependencies import Input, Output

# Import auxiliary functions from utils.py
from utils import get_color_gradient, range_label, determine_text_position, filter_dataframe_by_continent, load_figures, split_and_remove_duplicates_country, split_and_remove_duplicates_cities
from utils import c1, c2, c3, c4, c5, c6, c7, c8
from ewb_restapi_client import EWBRestapiClient

logging.basicConfig(level='DEBUG')
logger = logging.getLogger('Restapi')
restapi = EWBRestapiClient(logger)


# Initial data
df = pd.read_parquet('/data/source/SCOPUS.parquet/SCOPUS_5_percentage.parquet')

# ------------ TOP 25 CITIES ------------ #
# Aplica la función a cada fila
new_df = df.apply(split_and_remove_duplicates_cities, axis=1)
# Luego, puedes obtener el recuento de ciudades únicas
city_count = new_df.stack().value_counts()

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
df_fund_sponsor_count = df_fund_sponsor_count[df_fund_sponsor_count['fund_sponsor'] != ''].reset_index(drop=True).head(50).sort_values(by='num_projects', ascending=True)
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
# Aplica la función a cada fila
new_df = df.apply(split_and_remove_duplicates_country, axis=1)
# Luego, puedes obtener el recuento de ciudades únicas
country_count = new_df.stack().value_counts()

df_country_count = pd.DataFrame({'country': country_count.index, 'count': country_count.values})
df_country_count.drop(df_country_count[df_country_count['country'] == 'Spain'].index, inplace = True)

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
fig_map = px.choropleth(df_country_count,
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

# Initialize the app
app = Dash(__name__)

# App layout
app.layout = html.Div([
    html.H1('Bussiness Intelligence Dashboard for Scientific Publications', style={'text-align': 'center'}),

    # First row (bar charts stacked horizontally)
        html.Div([
        dcc.Graph(id='topics',figure=fig_fund_sponsor, style={'height': '700px', 'flex': '2'}),
        dcc.Graph(id='cities',figure=fig_cities, style={'height': '700px', 'flex': '1.5'}),
    ], style={'display': 'flex', 'flex-direction': 'row'}),

    html.Div([
        dcc.Graph(id='institutions',figure=fig_institutions, style={'height': '700px', 'flex': '1'}),
        dcc.Graph(id='fund',figure=fig_fund_sponsor, style={'height': '700px', 'flex': '1'})
    ], style={'display': 'flex', 'flex-direction': 'row'}),


    html.H2("Countries with the highest number of publications", style={'text-align': 'center'}),
    dcc.Dropdown(
        id='continent-dropdown',
        options=continent_options,
        value='world'  # Set the default value for the Dropdown
    ),
    dcc.Graph(id='choropleth-map', figure=fig_map),

    # Third row (pie charts stacked vertically and histogram)
    html.Div(children=[
        # First column (pie chart 1)
        dcc.Graph(id='openaccess',figure=fig_openaccess, style={'height': '400px', 'flex': '0.5'}),
        # Second column (pie chart 2)
        dcc.Graph(id='citedby',figure=fig_citedby, style={'height': '400px', 'flex': '0.5'}),
        dcc.Graph(id='years',figure=fig_years, style={'height': '400px', 'flex': '1'}),   
    ], style={'display': 'flex', 'flex-direction': 'row'}),

])

#---------------------------------------CALLBACKS--------------------------------------------#

# Callback to update the map based on the selected continent
@app.callback(
    Output('choropleth-map', 'figure'),
    [Input('continent-dropdown', 'value')]
)
def update_map(selected_continent):
    # Filter the DataFrame based on the selected continent
    filtered_df = filter_dataframe_by_continent(df_country_count, selected_continent)

    # Create a new choropleth map with the filtered DataFrame
    updated_fig = px.choropleth(filtered_df,
                        locations="country",
                        locationmode='country names',
                        color="count",
                        color_continuous_scale=px.colors.sequential.Emrld,
                        projection='natural earth',
                        scope=selected_continent,
                        title="Publications by Country")

    # Adjust the map size
    updated_fig.update_layout(
        mapbox=dict(
            center=dict(lat=0, lon=0),
            zoom=1.5,
            style="white-bg",
        ),
        height=600,
    )
    return updated_fig

""" 
# FIG OPENACCESS CALLBACK
@app.callback(
    [Output('fig_cities', 'figure'),
     Output('fig_institutions', 'figure'),
     # Agrega más salidas para otros gráficos que deseas actualizar
    ],
    [Input('fig_openaccess', 'clickData')]
)
def update_other_graphs(click_data):
    #if click_data is None:
        # Si no se ha hecho clic en ninguna categoría, muestra los gráficos por defecto
        # Puedes definir aquí cómo se deben mostrar los gráficos por defecto
        #fig_cities = generate_default_cities_figure()
        #fig_institutions = generate_default_institutions_figure()
        # Genera los otros gráficos por defecto si es necesario

    #else:
        # Obtiene la etiqueta de la categoría seleccionada
    selected_category = click_data['points'][0]['label']

    # Actualiza los gráficos según la categoría seleccionada
    if selected_category == 'Open Access':
        fig_cities = generate_cities_figure_for_open_access()
        fig_institutions = generate_institutions_figure_for_open_access()
        # Genera los otros gráficos para Open Access

    elif selected_category == 'Subscription':
        fig_cities = generate_cities_figure_for_subscription()
        fig_institutions = generate_institutions_figure_for_subscription()
        # Genera los otros gráficos para Subscription

    # Puedes agregar más lógica según las categorías que necesites

    return fig_cities, fig_institutions  # Agrega más gráficos actualizados aquí
"""
# Define el DataFrame inicial
#initial_df = df

# ...
"""     [Output('citedby', 'figure'),  # Cambia 'fig_cities' al ID correcto de tu gráfico
       # Cambia 'fig_institutions' al ID correcto de tu gráfico
     # Agrega más salidas para otros gráficos que desees actualizar
    ],
    [#Input('continent-dropdown', 'value'),
     Input('openaccess', 'clickData')  # Cambia 'fig_openaccess' al ID correcto de tu gráfico
    ] """

# Callback para actualizar el DataFrame y los gráficos
@app.callback(
    Output('citedby', 'figure'),  # Cambia 'citedby' al ID correcto de tu gráfico
    [Input('openaccess', 'clickData')] 
)
def update_data(openaccess_click):
    # Copia el DataFrame inicial para no modificarlo directamente
    updated_df = df.copy()

    # Actualiza el DataFrame según las selecciones del usuario
    if openaccess_click is not None:
        # Actualiza el DataFrame según la selección de openaccess_click
        # Puedes usar openaccess_click para filtrar el DataFrame de acuerdo a la selección del usuario
        # Obtiene la etiqueta de la categoría seleccionada
        selected_category = openaccess_click['points'][0]['label']
        #api_resp = restapi.open_access(selected_category=selected_category)
        api_resp = restapi.corpus_metadata()
        if api_resp.status_code != 200:
            logger.error(
                f"-- -- Error extracting SCOPUS from Solr")
        else:
            updated_df = pd.DataFrame(api_resp.results)
            logger.info("-- -- Data updated successfully")

    #if citedby_click is not None:
        # Actualiza el DataFrame según la selección de citedby_click
        # Puedes usar citedby_click para filtrar el DataFrame de acuerdo a la selección del usuario
        #selected_category = citedby_click['points'][0]['label']
        #updated_df = updated_df[actualiza según citedby_click]

    # Crea los gráficos actualizados con el DataFrame actualizado
    # Asegúrate de que los gráficos utilicen 'updated_df' en lugar del DataFrame original 'df'
    # ------------ CITED-BY COUNT PIE CHART ------------ #
    # Define the ranges
    rangos = [0, 5, 10, 50, updated_df['citedby_count'].max()]
    # Discretize the 'citedby_count' column
    updated_df['citedby_range'] = pd.cut(updated_df['citedby_count'], bins=rangos, right=False)
    # Count how many values fall into each range
    conteo_rangos = updated_df['citedby_range'].value_counts().reset_index()
    # Apply the function to get the labels of the ranges
    conteo_rangos['citedby_range'] = conteo_rangos['citedby_range'].apply(range_label)
    # Create the pie chart with plotly.express
    updated_fig_citedby = px.pie(conteo_rangos, names='citedby_range', values='count',
                        title= "Distribution of Number of Citations",
                        color_discrete_sequence=get_color_gradient(c7, c8, 4))
    # Personalize the labels directly in the graph
    updated_fig_citedby.update_traces(textposition='inside', textinfo='label', textfont_size=13.5, textfont_color='white', showlegend=False)
    updated_fig_citedby.update_layout(title_x=0.5)

    #updated_fig_map = px.choropleth(...
    #updated_fig_cities = px.bar(...
    #updated_fig_institutions = px.bar(...
    # Genera los otros gráficos actualizados aquí

    #return updated_fig_map, updated_fig_cities, updated_fig_institutions, updated_fig_citedby 
    return updated_fig_citedby




# Run the app
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)
    app.logger.info(f"-- -- AAAAAAAAAAAAA")
    """ api_resp = restapi.corpus_metadata()
    if api_resp.status_code != 200:
        logger.error(
            f"-- -- Error extracting SCOPUS from Solr")
    logger.info(f"-- -- Results from test query: {api_resp.results}")  """
    