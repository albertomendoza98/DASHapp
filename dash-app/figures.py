import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from utils import c1, c2, c3, c4, c5, c6, c7, c8
from utils import split_and_remove_duplicates_cities, split_and_remove_duplicates_country, get_color_gradient, determine_text_position, range_label

def load_figures(df: pd.DataFrame, continent: str):

    # ------------ TOP 25 CITIES ------------ #
    # Apply the function to each row
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
    fig_cities.update_traces(hoverlabel=dict(font=dict(size=20)))
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
    fig_institutions.update_traces(hoverlabel=dict(font=dict(size=20)))
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
    fig_fund_sponsor.update_traces(text=df_fund_sponsor_count['fund_sponsor'], textposition=[determine_text_position(value, scale_mid_fund) for value in df_fund_sponsor_count['num_projects']])
    fig_fund_sponsor.update_traces(hoverlabel=dict(font=dict(size=20)))
    fig_fund_sponsor.update_layout(yaxis_title='', yaxis_showline=False, yaxis_showticklabels=False, title_x=0.5, coloraxis_showscale=False)

    # ------------ OPEN-ACCESS PIE CHART ------------ #
    # Count the number of times each value appears in the 'openaccess' column
    counts = df['openaccess'].value_counts()
    fig_openaccess = px.pie(values=counts, names=counts.index, 
                title= "Distribution of Open Access Projects", 
                color_discrete_sequence=get_color_gradient(c5, c6, 2))
    # Personalize the labels directly in the graph
    fig_openaccess.update_traces(textposition='inside', textinfo='label', textfont_size=13.5, textfont_color='white', labels=['Open Access', 'Subscription'], showlegend=False)
    fig_openaccess.update_traces(hoverlabel=dict(font=dict(size=20)))
    fig_openaccess.update_layout(title_x=0.5)

    # ------------ CITED-BY COUNT PIE CHART ------------ #
    # Define the ranges
    rangos = [0, 5, 10, 25, df['citedby_count'].max()]
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
    fig_citedby.update_traces(hoverlabel=dict(font=dict(size=20)))
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
    fig_years.update_traces(hoverlabel=dict(font=dict(size=20)))
    fig_years.update_layout(title_x=0.5)

    # ---------------------- MAP ----------------------- #
    # Aplica la función a cada fila
    new_df = df.apply(split_and_remove_duplicates_country, axis=1)
    # Luego, puedes obtener el recuento de ciudades únicas
    country_count = new_df.stack().value_counts()

    df_country_count = pd.DataFrame({'country': country_count.index, 'count': country_count.values})
    df_country_count.drop(df_country_count[df_country_count['country'] == 'Spain'].index, inplace = True)

    # Create the choropleth map using Plotly Express (keep this outside the layout function)
    fig_map = px.choropleth(df_country_count,
                        locations="country",
                        locationmode='country names',
                        color="count",
                        color_continuous_scale=px.colors.sequential.Emrld,
                        projection='natural earth', 
                        scope=continent,
                        title="Publications by Country")
    
    fig_map.update_traces(hoverlabel=dict(font=dict(size=20)))

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

    return fig_cities, fig_institutions, fig_fund_sponsor, fig_openaccess, fig_citedby, fig_years, fig_map



def load_topic_map(df):
    # Crear un gráfico de dispersión
    fig_topic_map = go.Figure()

    for i, row in df.iterrows():
        fig_topic_map.add_trace(go.Scatter(
            x=[row['coords'][0]],
            y=[row['coords'][1]],
            mode='markers',
            name=row['tpc_labels'],
            marker=dict(
                size=row['ndocs_active'] / 600,
                opacity=0.5,
            ),
            showlegend=False,
            text=row['tpc_labels'],  # Asignar el valor de tpc_labels al atributo 'text'
            hoverinfo='text',  # Mostrar 'text' en el hover
            customdata=[row['tpc_labels']]  # Agregar 'tpc_labels' como dato personalizado
        ))

    # Actualizar la disposición del gráfico
    fig_topic_map.update_layout(title='Topic Map', title_x=0.5)
    fig_topic_map.update_xaxes(title_text='PC1')
    fig_topic_map.update_yaxes(title_text='PC2')

    # Aumentar el tamaño del texto en las etiquetas
    fig_topic_map.update_traces(hoverlabel=dict(font=dict(size=20)))

    return fig_topic_map


def load_updated_figures(df: pd.DataFrame, continent: str, trigger_id: str, 
                         fig_fund_sponsor, fig_openaccess, fig_citedby, fig_years):

    # ------------ TOP 25 CITIES ------------ #
    # Apply the function to each row
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
    fig_cities.update_traces(hoverlabel=dict(font=dict(size=20)))
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
    fig_institutions.update_traces(hoverlabel=dict(font=dict(size=20)))
    fig_institutions.update_layout(yaxis_title='', yaxis_showline=False, yaxis_showticklabels=False, title_x=0.5, coloraxis_showscale=False)

    if(trigger_id != 'fund'):
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
        fig_fund_sponsor.update_traces(text=df_fund_sponsor_count['fund_sponsor'], textposition=[determine_text_position(value, scale_mid_fund) for value in df_fund_sponsor_count['num_projects']])
        fig_fund_sponsor.update_traces(hoverlabel=dict(font=dict(size=20)))
        fig_fund_sponsor.update_layout(yaxis_title='', yaxis_showline=False, yaxis_showticklabels=False, title_x=0.5, coloraxis_showscale=False)

    if(trigger_id != 'openaccess'):
        # ------------ OPEN-ACCESS PIE CHART ------------ #
        # Count the number of times each value appears in the 'openaccess' column
        counts = df['openaccess'].value_counts()
        fig_openaccess = px.pie(values=counts, names=counts.index, 
                    title= "Distribution of Open Access Projects", 
                    color_discrete_sequence=get_color_gradient(c5, c6, 2))
        # Personalize the labels directly in the graph
        fig_openaccess.update_traces(textposition='inside', textinfo='label', textfont_size=13.5, textfont_color='white', labels=['Open Access', 'Subscription'], showlegend=False)
        fig_openaccess.update_traces(hoverlabel=dict(font=dict(size=20)))
        fig_openaccess.update_layout(title_x=0.5)

    if(trigger_id != 'citedby'):
        # ------------ CITED-BY COUNT PIE CHART ------------ #
        # Define the ranges
        rangos = [0, 5, 10, 25, df['citedby_count'].max()]
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
        fig_citedby.update_traces(hoverlabel=dict(font=dict(size=20)))
        fig_citedby.update_layout(title_x=0.5)

    if(trigger_id != 'years'):    
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
        fig_years.update_traces(hoverlabel=dict(font=dict(size=20)))
        fig_years.update_layout(title_x=0.5)

    # ---------------------- MAP ----------------------- #
    # Aplica la función a cada fila
    new_df = df.apply(split_and_remove_duplicates_country, axis=1)
    # Luego, puedes obtener el recuento de ciudades únicas
    country_count = new_df.stack().value_counts()

    df_country_count = pd.DataFrame({'country': country_count.index, 'count': country_count.values})
    df_country_count.drop(df_country_count[df_country_count['country'] == 'Spain'].index, inplace = True)

    # Create the choropleth map using Plotly Express (keep this outside the layout function)
    fig_map = px.choropleth(df_country_count,
                        locations="country",
                        locationmode='country names',
                        color="count",
                        color_continuous_scale=px.colors.sequential.Emrld,
                        projection='natural earth', 
                        scope=continent,
                        title="Publications by Country")
    
    fig_map.update_traces(hoverlabel=dict(font=dict(size=20)))

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

    return fig_cities, fig_institutions, fig_fund_sponsor, fig_openaccess, fig_citedby, fig_years, fig_map
