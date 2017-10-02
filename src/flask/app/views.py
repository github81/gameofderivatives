# jsonify creates a json representation of the response
from flask import jsonify
from flask import render_template

from app import app
import redis
import pandas as pd

# importing Cassandra modules from the driver we just installed
from cassandra.cluster import Cluster

# Setting up connections to cassandra

# Change the bolded text to your seed node public dns (no < or > symbols but keep quotations. Be careful to copy quotations as it might copy it as a special character and throw an error. Just delete the quotations and type them in and it should be fine. Also delete this comment line
cluster = Cluster(['ec2-34-235-99-222.compute-1.amazonaws.com'])
session = cluster.connect('swappg')

@app.route('/')
@app.route('/index')
def index():
    return render_template('index.html')

@app.route('/organization/<organization>')
def get_id(organization):
    stmt = "SELECT * FROM swap WHERE organization=%s"
    response = session.execute(stmt, parameters=[organization])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"id": x.id, "organization": x.organization, "valuationdate": x.valuationdate, "swapvalue": x.swapvalue} for x in response_list]
    return jsonify(swaps=jsonresponse)


@app.route('/swapvalue/<id>')
def get_swap_value(id):
    cql = "select * from swap where id=%s"
    response = session.execute(cql, parameters=[])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"id": x.id, "organization": x.organization, "valuationdate": x.valuationdate, "swapvalue": x.swapvalue} for x in response_list]
    return jsonify(swaps=jsonresponse)

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def cql_to_df(cql):
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    stmt = session.execute(cql)
    df = stmt._current_rows
    return df


@app.route('/portfoliovalue')
def get_portfolio_value():
    
    #get data from the cassandra to df
    cql = "select id, organization, payccy, receiveccy, swapvalue, max(valuationdate) from swap group by organization, id, payccy, receiveccy"
    df = cql_to_df(cql)
    
    #group by swap values by orgnaziation => portfolio value
    #and also by payccy and orgnaization
    df_ccy = df.groupby(['organization','payccy'])['swapvalue'].sum().reset_index()
    df_org = df.groupby(['organization'])['swapvalue'].sum().reset_index()
                    
    df_org['portfoliovalue'] = df_org.apply(lambda x:'{name: \'%s\', y: %s, drilldown:true}' % (x['organization'],x['swapvalue']/100000000),axis=1)
    df_org = df_org.sort('swapvalue', ascending=False)
    portfoliovalues=""
    portfoliodrilldowns=""
    commaflag=0
    for index,row in df_org.iterrows():
        portfoliovalues += row['portfoliovalue'] + ","
        df_2 = df_ccy.loc[df_ccy['organization'] == row['organization']]
        drilldown=""
        if commaflag==1:
            drilldown = ","
        drilldown += "'" + row['organization'] + "': { "
        if commaflag==0:
            commaflag=1
        drilldown += "name: '" + row['organization'] + "',"
        drilldown += "data: ["
        for index_2,row_2 in df_2.iterrows():
                #for individual drilldowns
                drilldown += "['" + row_2['payccy'] + "'," + str(row_2['swapvalue']/100000000) + "],"
        drilldown = drilldown[:-1]
        drilldown += "]}"
        portfoliodrilldowns += drilldown
                    
    #remove the last character (,) from porfolio drill downs string
    #portfoliodrilldowns = portfoliodrilldowns[:-1]
    return render_template('index.html', portfoliovalues=portfoliovalues, portfoliodrilldowns=portfoliodrilldowns)


