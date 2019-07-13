import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from databaseConnection import dataBaseConnect
import psycopg2 as ps2
from pandas import DataFrame

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.css.append_css({'external_url': 'https://codepen.io/amyoshino/pen/jzXypZ.css'})  # noqa: E501
server = app.server

colors = {
    'green' : '003300',
    'lightGreen' : '#99ff33',
    'blue' : '#000066',
    'lightBlue' : '#66ffcc'
}

def getStatesName():
    query = """
    select state_name  from states_48;
    """
    return makeDataframeFromQuery(query)

def getAnnualData(tableName, stateName):
    query = """
            select from_1919_1938, from_1939_1958, from_1959_1978, 
            from_1979_1998, from_1999_2018 from  {} where state_name = '{}';
            """.format(tableName, stateName)
    return makeDataframeFromQuery(query)

def getMonthlyData(tableName, interval, climateItem):
    query = """
            select month, interval, avg({}) as {} from {} where interval = '{}'  group by month, interval order by month;
            """.format(climateItem, climateItem, tableName, interval)
    return  makeDataframeFromQuery(query)

def getElevationRangesData(tableName):
    query = """
            select * from {};
            """.format(tableName)
    return  makeDataframeFromQuery(query)

def makeDataframeFromQuery(query):
    newConection = dataBaseConnect().connectToDataBase()
    dataframe = pd.read_sql(query, newConection)
    newConection.close()
    return dataframe


statesName = getStatesName()

app.layout = html.Div(children=[
    html.Div([
        html.H1('Climate Watch', style={"backgroundColor": colors['lightGreen'] ,'textAlign': 'center','color': colors['green']})
    ], className="row"),
    html.Div([
        dcc.Dropdown(
        id = 'select_state',
        options = [{"label": l, "value": l} for l in statesName.state_name],
        placeholder = "Select a State")
    ], className="row" ), 
    html.Div([
        html.H2('Annual Summary', style={"backgroundColor": colors['lightBlue'] ,'textAlign': 'center','color': colors['blue']})
    ], className="row"),

    html.Div([
        html.Div(
             dcc.Graph(
                id ="tmax",
                className = "four columns"
             )
        ),
        html.Div(
           dcc.Graph(
              id ="tmin",
              className = "four columns"
          )
        ),
        html.Div(
           dcc.Graph(
              id ="prcp",
              className = "four columns"
            )
        )
    ], className="row" ),
    html.Div([
         html.H2('Monthly Summary', style={"backgroundColor": colors['lightBlue'] ,'textAlign': 'center','color': colors['blue']})
     ], className="row" ),
     html.Div([
         html.Div(
             dcc.Graph(
                 id ="monthly_tmax"
             ),
         ),
         html.Div(
             dcc.Graph(
                 id ="monthly_tmin"
             ),
         ),
         html.Div(
             dcc.Graph(
                 id ="monthly_prcp"
             ),
         )
     ], className="row"), 
     html.Div([
         html.H2('Elevation Ranges', style={"backgroundColor": colors['lightBlue'] ,'textAlign': 'center','color': colors['blue']})
     ], className="row" ),
     html.Div([
         html.Div(
             dcc.Graph(
                 id = "tmax_elevation_ranges"
             ),
         ),
         html.Div(
             dcc.Graph(
                 id = "tmin_elevation_ranges"
             ),
         ),
         html.Div(
             dcc.Graph(
                 id = "prcp_elevation_ranges"
             ),
         ),


     ], className="row" )
])

state_neme = "California"
@app.callback(
     Output("tmax", "figure"),
     [Input("select_state", "value")])
def annualTmaxBar(state_name):
    values = getAnnualData('annual_tmax', state_name)
    values =  values.values.flatten()
    figure = go.Figure(
        data = [go.Bar(
            x = ['1919-1938', '1939-1958', '1959-1978', '1979-1998', '1999-2018'],
            y = values,
            marker=dict(
            color=['rgba(66,135,245,1)', 'rgba(96, 245, 66,1)',
                   'rgba(232,62,247,1)', 'rgba(247, 182, 62,1)',
                   'rgba(18,8,79,1)']),

            )],
        layout=go.Layout(
            title="Maximum Temperature",
            xaxis=dict(title = '20 Year Intervals'),
            yaxis=dict(title = 'Temperature C')
        )

    )
    return figure

@app.callback(
     Output("tmin", "figure"),
     [Input("select_state", "value")])
def annualTminBar(state_name):
    values = getAnnualData('annual_tmin', state_name)
    values =  values.values.flatten()
    figure = go.Figure(
        data = [go.Bar(
            x = ['1919-1938', '1939-1958', '1959-1978', '1979-1998', '1999-2018'],
            y = values,
            marker=dict(
            color=['rgba(66,135,245,1)', 'rgba(96, 245, 66,1)',
                   'rgba(232,62,247,1)', 'rgba(247, 182, 62,1)',
                   'rgba(18,8,79,1)']),
            )],
        layout=go.Layout(
            title="Minimum Temperature",
            xaxis=dict(title = '20 Year Intervals'),
            yaxis=dict(title = 'Temperature C')
        ),

    )
    return figure

@app.callback(
     Output("prcp", "figure"),
     [Input("select_state", "value")])
def annualPrcpBar(state_name):
    values = getAnnualData('annual_prcp', state_name)
    values =  values.values.flatten()
    figure = go.Figure(
        data = [go.Bar(
            x = ['1919-1938', '1939-1958', '1959-1978', '1979-1998', '1999-2018'],
            y = values,
            marker=dict(
            color=['rgba(66,135,245,1)', 'rgba(96, 245, 66,1)',
                   'rgba(232,62,247,1)', 'rgba(247, 182, 62,1)',
                   'rgba(18,8,79,1)']),
            )],
        layout=go.Layout(
            title="Precipitation",
            xaxis=dict(title = '20 Year Interval'),
            yaxis=dict(title = 'Precipitation mm')
        ),
     )
    return figure


@app.callback(
     Output("monthly_tmax", "figure"),
     [Input("select_state", "value")])
def monthlyTmaxBar(state_name):
    xAxis = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun','Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    tableName = "monthly_tmax"
    climateItem = "tmax"
    from_1919_1938 = getMonthlyData(tableName, "1919_1938" ,climateItem)[climateItem].tolist()
    from_1939_1958 = getMonthlyData(tableName, "1939_1958" ,climateItem)[climateItem].tolist()
    from_1959_1978 = getMonthlyData(tableName, "1959_1978" ,climateItem)[climateItem].tolist()
    from_1979_1998 = getMonthlyData(tableName, "1979_1998" ,climateItem)[climateItem].tolist()
    from_1999_2018 = getMonthlyData(tableName, "1999_2018" ,climateItem)[climateItem].tolist()


    interval_1 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1919-1938'
    )
    interval_2 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1939-1958'
    )
    interval_3 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1959-1978'
    )
    interval_4 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1979-1998'
    )
    interval_5 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1999-2018'
    )

    data = [interval_1, interval_2, interval_3, interval_4, interval_5]
    layout = go.Layout(
        barmode='group',
        showlegend = True,
        title="Maximum Temperature",
        xaxis=dict(title = 'Month'),
        yaxis=dict(title = 'Temperature C')
    )
    figure = go.Figure(data=data, layout=layout)
    return figure


@app.callback(
     Output("monthly_tmin", "figure"),
     [Input("select_state", "value")])
def monthlyTminBar(state_name):
    xAxis = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun','Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    tableName = "monthly_tmin"
    climateItem = "tmin"
    from_1919_1938 = getMonthlyData(tableName, "1919_1938" ,climateItem)[climateItem].tolist()
    from_1939_1958 = getMonthlyData(tableName, "1939_1958" ,climateItem)[climateItem].tolist()
    from_1959_1978 = getMonthlyData(tableName, "1959_1978" ,climateItem)[climateItem].tolist()
    from_1979_1998 = getMonthlyData(tableName, "1979_1998" ,climateItem)[climateItem].tolist()
    from_1999_2018 = getMonthlyData(tableName, "1999_2018" ,climateItem)[climateItem].tolist()

    interval_1 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1919-1938'
    )
    interval_2 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1939-1958'
    )
    interval_3 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1959-1978'
    )
    interval_4 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1979-1998'
    )
    interval_5 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1999-2018'
    )

    data = [interval_1, interval_2, interval_3, interval_4, interval_5]
    layout = go.Layout(
        barmode='group',
        showlegend = True,
        title="Minimum Temperature",
        xaxis=dict(title = 'Month'),
        yaxis=dict(title = 'Temperature C')
    )
    figure = go.Figure(data=data, layout=layout)
    return figure


@app.callback(
     Output("monthly_prcp", "figure"),
     [Input("select_state", "value")])
def monthlyPrcpBar(state_name):
    xAxis = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun','Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
    tableName = "monthly_prcp"
    climateItem = "prcp"
    from_1919_1938 = getMonthlyData(tableName, "1919_1938" ,climateItem)[climateItem].tolist()
    from_1939_1958 = getMonthlyData(tableName, "1939_1958" ,climateItem)[climateItem].tolist()
    from_1959_1978 = getMonthlyData(tableName, "1959_1978" ,climateItem)[climateItem].tolist()
    from_1979_1998 = getMonthlyData(tableName, "1979_1998" ,climateItem)[climateItem].tolist()
    from_1999_2018 = getMonthlyData(tableName, "1999_2018" ,climateItem)[climateItem].tolist()

    interval_1 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1919-1938'
    )
    interval_2 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1939-1958'
    )
    interval_3 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1959-1978'
    )
    interval_4 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1979-1998'
    )
    interval_5 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1999-2018'
    )

    data = [interval_1, interval_2, interval_3, interval_4, interval_5]
    layout = go.Layout(
        barmode='group',
        showlegend = True,
        title="Precipitation",
        xaxis=dict(title = 'Month'),
        yaxis=dict(title = 'Precipitation mm')
    )
    figure = go.Figure(data=data, layout=layout)
    return figure


@app.callback(
     Output("tmax_elevation_ranges", "figure"),
     [Input("select_state", "value")])
def elevationRangesTmaxBar(state_name):
    xAxis = ['0-200', '201-400', '401-600', '601-800', '801-1000', '1001-1200', '1201-1400', '1401-1600',\
             '1601-1800', '1801-2000', '2001-2200', '2201-2400', '2401-2600', '2601-2800', '2801-3000',\
             '3001-3200', '3201-3400', '3401-3600', '3601-3800', '3801-4000', '4001-4200', '4201-4400']
    tableName = "tmax_elev_ranges"
    from_1919_1938 = getElevationRangesData(tableName)["from_1919_1938"].tolist()
    from_1939_1958 = getElevationRangesData(tableName)["from_1939_1958"].tolist()
    from_1959_1978 = getElevationRangesData(tableName)["from_1959_1978"].tolist()
    from_1979_1998 = getElevationRangesData(tableName)["from_1979_1998"].tolist()
    from_1999_2018 = getElevationRangesData(tableName)["from_1999_2018"].tolist()

    interval_1 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1919-1938'
    )
    interval_2 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1939-1958'
    )
    interval_3 = go.Bar(
        x = xAxis,
        y = from_1959_1978,
        name = 'From 1959-1978'
    )
    interval_4 = go.Bar(
        x = xAxis,
        y = from_1979_1998,
        name = 'From 1979-1998'
    )
    interval_5 = go.Bar(
        x = xAxis,
        y = from_1999_2018,
        name = 'From 1999-2018'
    )

    data = [interval_1, interval_2, interval_3, interval_4, interval_5]
    layout = go.Layout(
        barmode='group',
        showlegend = True,
        title="Elevation Ranges Vs Maximum Temperature",
        xaxis=dict(title = 'Elevation Ranges m'),
        yaxis=dict(title = 'Temperature C')
    )
    figure = go.Figure(data=data, layout=layout)
    return figure


@app.callback(
     Output("tmin_elevation_ranges", "figure"),
     [Input("select_state", "value")])
def elevationRangesTminBar(state_name):
    xAxis = ['0-200', '201-400', '401-600', '601-800', '801-1000', '1001-1200', '1201-1400', '1401-1600',\
             '1601-1800', '1801-2000', '2001-2200', '2201-2400', '2401-2600', '2601-2800', '2801-3000',\
             '3001-3200', '3201-3400', '3401-3600', '3601-3800', '3801-4000', '4001-4200', '4201-4400']
    tableName = "tmin_elev_ranges"
    from_1919_1938 = getElevationRangesData(tableName)["from_1919_1938"].tolist()
    from_1939_1958 = getElevationRangesData(tableName)["from_1939_1958"].tolist()
    from_1959_1978 = getElevationRangesData(tableName)["from_1959_1978"].tolist()
    from_1979_1998 = getElevationRangesData(tableName)["from_1979_1998"].tolist()
    from_1999_2018 = getElevationRangesData(tableName)["from_1999_2018"].tolist()

    interval_1 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1919-1938'
    )
    interval_2 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1939-1958'
    )
    interval_3 = go.Bar(
        x = xAxis,
        y = from_1959_1978,
        name = 'From 1959-1978'
    )
    interval_4 = go.Bar(
        x = xAxis,
        y = from_1979_1998,
        name = 'From 1979-1998'
    )
    interval_5 = go.Bar(
        x = xAxis,
        y = from_1999_2018,
        name = 'From 1999-2018'
    )

    data = [interval_1, interval_2, interval_3, interval_4, interval_5]
    layout = go.Layout(
        barmode='group',
        showlegend = True,
        title="Elevation Ranges Vs Minimum Temperature",
        xaxis=dict(title = 'Elevation Ranges'),
        yaxis=dict(title = 'Temperature C')
    )
    figure = go.Figure(data=data, layout=layout)
    return figure


@app.callback(
     Output("prcp_elevation_ranges", "figure"),
     [Input("select_state", "value")])
def elevationRangesPrcpBar(state_name):
    xAxis = ['0-200', '201-400', '401-600', '601-800', '801-1000', '1001-1200', '1201-1400', '1401-1600',\
             '1601-1800', '1801-2000', '2001-2200', '2201-2400', '2401-2600', '2601-2800', '2801-3000',\
             '3001-3200', '3201-3400', '3401-3600', '3601-3800', '3801-4000', '4001-4200', '4201-4400']
    tableName = "prcp_elev_ranges"
    from_1919_1938 = getElevationRangesData(tableName)["from_1919_1938"].tolist()
    from_1939_1958 = getElevationRangesData(tableName)["from_1939_1958"].tolist()
    from_1959_1978 = getElevationRangesData(tableName)["from_1959_1978"].tolist()
    from_1979_1998 = getElevationRangesData(tableName)["from_1979_1998"].tolist()
    from_1999_2018 = getElevationRangesData(tableName)["from_1999_2018"].tolist()

    interval_1 = go.Bar(
        x = xAxis,
        y = from_1919_1938,
        name = 'From 1919-1938'
    )
    interval_2 = go.Bar(
        x = xAxis,
        y = from_1939_1958,
        name = 'From 1939-1958'
    )
    interval_3 = go.Bar(
        x = xAxis,
        y = from_1959_1978,
        name = 'From 1959-1978'
    )
    interval_4 = go.Bar(
        x = xAxis,
        y = from_1979_1998,
        name = 'From 1979-1998'
    )
    interval_5 = go.Bar(
        x = xAxis,
        y = from_1999_2018,
        name = 'From 1999-2018'
    )

    data = [interval_1, interval_2, interval_3, interval_4, interval_5]
    layout = go.Layout(
        barmode='group',
        showlegend = True,
        title="Elevation Ranges Vs Precipitation",
        xaxis=dict(title = 'Elevation Ranges m'),
        yaxis=dict(title = 'Precipitation mm')
    )
    figure = go.Figure(data=data, layout=layout)
    return figure


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port='5656' , debug=True)



