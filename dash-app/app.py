# Import packages
from dash import Dash, html, dcc, callback_context
import pandas as pd
import numpy as np
import logging
import time

from dash.dependencies import Input, Output

# Import auxiliary functions from utils.py
from utils import continent_options
from figures import load_figures, load_topic_map, load_updated_figures
from ewb_restapi_client import EWBRestapiClient

logging.basicConfig(level='DEBUG')
logger = logging.getLogger('Restapi')
restapi = EWBRestapiClient(logger)

#Give enough time to initialize Solr
time = time.sleep(20)

df = pd.read_parquet('/data/source/SCOPUS.parquet/SCOPUS_BIGDATA_5.parquet')

# Load Figures
fig_cities, fig_institutions, fig_fund_sponsor, fig_openaccess, fig_citedby, fig_years, fig_map = load_figures(df=df, continent='world')

# ---------------------- TOPIC MAP ----------------------- #
# Retrieve model information
api_resp = restapi.topic_map()
if api_resp.status_code != 200:
    logger.error(
        f"-- -- Error extracting SCOPUS from Solr")
else:
    df_topic = pd.DataFrame(api_resp.results)

# Load Topic Map
fig_topic_map = load_topic_map(df_topic)

# Initialize the app
app = Dash(__name__)

# App layout
app.layout = html.Div([
    html.H1('Bussiness Intelligence Dashboard for Scientific Publications', style={'text-align': 'center'}),

    # First row (topic map and bar chart)
        html.Div([
        dcc.Graph(id='topic-map',figure=fig_topic_map, style={'height': '700px', 'flex': '2'}),
        dcc.Graph(id='cities',figure=fig_cities, style={'height': '700px', 'flex': '1.5'}),
    ], style={'display': 'flex', 'flex-direction': 'row'}),

    # Second row (bar charts stacked horizontally)
    html.Div([
        dcc.Graph(id='institutions',figure=fig_institutions, style={'height': '700px', 'flex': '1'}),
        dcc.Graph(id='fund',figure=fig_fund_sponsor, style={'height': '700px', 'flex': '1'})
    ], style={'display': 'flex', 'flex-direction': 'row'}),

    # Third row (choropleth-map)
    html.H2("Countries with the highest number of publications", style={'text-align': 'center'}),
    dcc.Dropdown(
        id='continent-dropdown',
        options=continent_options,
        value='world'  # Set the default value for the Dropdown
    ),
    dcc.Graph(id='choropleth-map', figure=fig_map),

    # Fourth row (pie charts stacked vertically and histogram)
    html.Div(children=[
        # First column (pie chart 1)
        dcc.Graph(id='openaccess',figure=fig_openaccess, style={'height': '400px', 'flex': '0.5'}),
        # Second column (pie chart 2)
        dcc.Graph(id='citedby',figure=fig_citedby, style={'height': '400px', 'flex': '0.5'}),
        dcc.Graph(id='years',figure=fig_years, style={'height': '400px', 'flex': '1'}),   
    ], style={'display': 'flex', 'flex-direction': 'row'}),

])

#---------------------------------------CALLBACKS--------------------------------------------#
@app.callback(
    [
        Output('cities', 'figure'),
        Output('choropleth-map', 'figure'),
        Output('institutions', 'figure'),
        Output('fund', 'figure'),
        Output('years', 'figure'), 
        Output('openaccess', 'figure'),
        Output('citedby', 'figure') 
    ],
    [
        Input('cities', 'clickData'),
        Input('topic-map', 'clickData'),
        Input('institutions', 'clickData'),
        Input('fund', 'clickData'),
        Input('years', 'clickData'), 
        Input('openaccess', 'clickData'),
        Input('citedby', 'clickData'),
        Input('continent-dropdown', 'value')
    ]
)
def update_data(click_data_cities, click_data_topicmap, click_data_institutions,
                click_data_fund, click_data_years, click_data_openaccess, 
                click_data_citedby, selected_continent):
    updated_df = df.copy()
    trigger_id='all'
    ctx = callback_context

    if ctx.triggered:
        trigger_id = ctx.triggered[0]['prop_id'].split('.')[0]

        if trigger_id == 'cities' and click_data_cities:
            city = click_data_cities['points'][0]['label']
            api_resp = restapi.city(city=city)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'topic-map' and click_data_topicmap:
            topic_label = click_data_topicmap['points'][0]['customdata']
            api_resp = restapi.topic_label(topic_label=topic_label)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'institutions' and click_data_institutions:
            institution = click_data_institutions['points'][0]['label']
            api_resp = restapi.institution(institution=institution)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'fund' and click_data_fund:
            fund = click_data_fund['points'][0]['label']
            api_resp = restapi.fund(fund=fund)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'years' and click_data_years:
            year = click_data_years['points'][0]['x']
            api_resp = restapi.year(year=year)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'openaccess' and click_data_openaccess:
            selected_category = click_data_openaccess['points'][0]['label']
            api_resp = restapi.open_access(selected_category=selected_category)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'citedby' and click_data_citedby:
            label = click_data_citedby['points'][0]['label']
            api_resp = restapi.cited_count(label=label)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)

        elif trigger_id == 'continent-dropdown' and selected_continent:
            api_resp = restapi.continent(continent=selected_continent)
            if api_resp.status_code != 200:
                logger.error(
                    f"-- -- Error extracting SCOPUS from Solr")
            else:
                updated_df = pd.DataFrame(api_resp.results)
    
    updated_fig_cities, updated_fig_institutions, updated_fig_fund_sponsor, updated_fig_openaccess , updated_fig_citedby, updated_fig_years, updated_fig_map = load_updated_figures(df=updated_df, continent = selected_continent, trigger_id=trigger_id,
                                                                                                                                                                                       fig_fund_sponsor=fig_fund_sponsor, fig_openaccess=fig_openaccess, 
                                                                                                                                                                                       fig_citedby=fig_citedby, fig_years=fig_years)

    # Devolver las figuras actualizadas
    return updated_fig_cities, updated_fig_map, updated_fig_institutions, updated_fig_fund_sponsor, updated_fig_years, updated_fig_openaccess, updated_fig_citedby


# Run the app
if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port=8050, debug=True)