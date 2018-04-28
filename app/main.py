# Copyright 2018 Ct,innovation. All Rights Reserved.
#
import datetime
import json
from flask import Flask
from flask import request
import sportslive
from google.cloud import bigquery
from google.cloud import storage

app = Flask(__name__)

SL = sportslive.SportsLive()

@app.route('/news-loader', methods=['GET'])
def newsloader():
    """Given an query, return that news."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    rowcount = int(querylist[1])
    day = querylist[2]

    if query is None:
      return 'No provided.', 400
    if rowcount is None:
        rowcount = 2
    if day is None:
        day = datetime.date.today()
        day = day.strftime('%Y%m%d')
    result = SL.news_loader(query, rowcount, day)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/news-loader', methods=['GET'])
def newsloader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    rowcount = int(querylist[1])
    day = querylist[2]

    if query is None:
      return 'No provided.', 400
    if rowcount is None:
        rowcount = 2
    if day is None:
        day = datetime.date.today()
        day = day.strftime('%Y%m%d')
    result = SL.news_loader(query, rowcount, day, debug=True)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


@app.route('/player-loader', methods=['GET'])
def playerloader():
    """Given an query, return that news."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = querylist[1]
    
    if query is None:
        return 'No provided.', 400
    if day is None:
        day = datetime.date.today()
        day = day.strftime('%Y%m%d')
        
    result = SL.player_loader(query, day)
    if result is None:
        return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/player-loader', methods=['GET'])
def playerloader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    day = querylist[1]

    if query is None:
      return 'No provided.', 400
    if day is None:
        day = datetime.date.today()
        day = day.strftime('%Y%m%d')
        
    result = SL.player_loader(query, day, debug=True)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


@app.route('/news-reader', methods=['GET'])
def newsreader():
    """Given an query, return that news."""
    query = request.args.get('query')
    if query is None:
      return 'No provided.', 400
    result = SL.news_check(query)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


@app.route('/debug/news-reader', methods=['GET'])
def newsreader_debug():
    """Given an query, return that news debug mode."""
    query = request.args.get('query')
    if query is None:
      return 'No provided.', 400
    result = SL.news_check(query, debug=True)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


@app.route('/sammarize', methods=['GET'])
def summarize():
    """Given an query, return that news."""
    query = request.args.get('query')
    querylist = query.split('_')
    query = querylist[0]
    rowcount = int(querylist[1])
    
    if query is None:
      return 'No provided.', 400
    result = SL.sammarize(query, rowcount)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


@app.route('/add-record', methods=['GET'])
def add_record():
    json_dict = {}
    ra = sportslive.RecordAccumulation()
    """Given an date, records add to table ."""
    day = None
    try:
        day = request.args.get('query').split('-')
        if day is None:
            return 'No provided.', 400
    except:
        json_dict.update({'error':
                         {
                         'text':'date dont get it'
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data
    
    try:
        day = datetime.date(int(day[0]), int(day[1]), int(day[2]))
        tdatetime = day.strftime('%Y%m%d')
    except:
        json_dict.update({'error':
                         {
                         'text':type(day)
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data
    
    try:
        news_record = ra.news_check(day)
        # ra.save_csv(news_record, "news_record.csv")

        result = load_data("sportsagent",
                           "newsrecord${}".format(tdatetime),
                           news_record)
    except:
        json_dict.update({'error':
                         {
                         'text':'Dont get the news'
                         }}
                         )
        encode_json_data = json.dumps(json_dict)
        return encode_json_data        
        
    if result:
        return 'not found : %s' % day, 400

    player_list = ra.get_player_dic(day)
    player_record = ra.get_player_record(player_list, day)
    # ra.save_csv(player_record, "player_record.csv")

    result = load_data("sportsagent",
                       "playerrecord${}".format(tdatetime),
                       player_record)

    if result:
        return 'not found : %s' % day, 400
    return result, 200


def load_data(dataset_id, table_id, source):
    storage_client = storage.Client()
    # Make an authenticated API request
    buckets = list(storage_client.list_buckets())
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    # with open(source_file_name, 'rb') as source_file:
    # See https://cloud.google.com/bigquery/loading-data
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = 'text/csv'
    job = bigquery_client.load_table_from_file(
            source, table_ref, job_config=job_config)

    job.result()  # Waits for job to complete

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

    return job.done()


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)

