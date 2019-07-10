import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
from databaseConnection import dataBaseConnect
import psycopg2 as ps2

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

def makeDataframeFromQuery(query):
    newConection = dataBaseConnect().connectToDataBase()
    dataframe = pd.read_sql(query, newConection)
    newConection.close()
    return dataframe


statesName = getStatesName()
#print(statesName)
#test = getAnnualData('annual_tmax', 'California')
#print(test)
#print(type(test))
#test.to_dict()
#test.drop(columns=['short_name', 'state_name'], inplace=True)
#test.drop([0, 6])
#test = test.drop(columns=['short_name', 'state_name'])
#print(test)
#print(type(test['from_1919_1938']))

app.layout = html.Div(children=[
    html.Div(
        html.H1('Climate Watch', style={"backgroundColor": colors['lightGreen'] ,'textAlign': 'center','color': colors['green']})
    ),
    html.Div([
        dcc.Dropdown(
        id = 'select_state',
        options = [{"label": l, "value": l} for l in statesName.state_name],
        placeholder = "Select a State")

    ]), 
    html.Div(
        html.H2('Annual Summary', style={"backgroundColor": colors['lightBlue'] ,'textAlign': 'center','color': colors['blue']})
    ),



])


@app.callback(
     Output("main-graph", "figure"),
     [Input("select_state",'value')])
def annualTmaxBar(stateName):
    print(statesName)

   # chart = getAnnualData("annual_tmax", statename)
   # xaxis = chart.columns
   # data = [
   # go.Bar(
   #     x=xaxis
   # )
   #]


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', port='5656' , debug=True)


