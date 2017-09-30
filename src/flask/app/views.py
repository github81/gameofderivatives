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

@app.route('/api/<organization>')
def get_id(organization):
    stmt = "SELECT * FROM swap WHERE organization=%s"
    response = session.execute(stmt, parameters=[organization])
    response_list = []
    for val in response:
        response_list.append(val)
    jsonresponse = [{"id": x.id, "organization": x.organization, "valuationdate": x.valuationdate, "swapvalue": x.swapvalue} for x in response_list]
    return jsonify(swaps=jsonresponse)


@app.route('/swapvalue')
def get_swap_value():
    cql = "select organization, sum(swapvalue) as portfolio_value from swap group by organization"
    stmt = session.execute(cql, parameters=[])
    responselist = []
    response = []
    for val in stmt:
        response.append(val)
    jsonresponse = [{"organization": x.organization, "portfolio value": x.portfolio_value} for x in response]
    return jsonify(swaps=jsonresponse)

@app.route('/portfoliovalue-orig')
def get_portfolio_value_orig():
    cql = "select id, organization, swapvalue, max(valuationdate) from swap group by organization,id,payccy,receiveccy"
    stmt = session.execute(cql, parameters=[])
    responselist = []
    portfoliovalues = []
    response = []
    portfoliovalues = ""
    count = 0
    for val in stmt:
        portfoliovalues += "['" + str(val.organization) + "'," + str(val.swapvalue/1000000) + "],"
        if count == 750:
            break
        count += 1
    return render_template('index.html', portfoliovalues=portfoliovalues)

@app.route('/portfoliovalue')
def get_portfolio_value():
    
    def pandas_factory(colnames, rows):
        return pd.DataFrame(rows, columns=colnames)

    #get data from the cassandra to df
    cql = "select id, organization, swapvalue, max(valuationdate) from swap group by organization, id, payccy, receiveccy"
    session.row_factory = pandas_factory
    session.default_fetch_size = None
    stmt = session.execute(cql)
    df = stmt._current_rows
    
    #group by swap values by orgnaziation => portfolio value
    df = df.groupby(['organization'])['swapvalue'].sum().reset_index()
    df['portfoliovalue'] = df.apply(lambda x:'[\'%s\',%s]' % (x['organization'],x['swapvalue']/10000000),axis=1)
    df = df.sort('swapvalue', ascending=False)
    portfoliovalues=""
    for index,row in df.iterrows():
        portfoliovalues += row['portfoliovalue'] + ","
    return render_template('index.html', portfoliovalues=portfoliovalues)


