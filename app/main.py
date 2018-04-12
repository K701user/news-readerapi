# Copyright 2018 Ct,innovation. All Rights Reserved.
#

from flask import Flask
from flask import request
import sportslive

app = Flask(__name__)

SL = sportslive.SportsLive()

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
    rowcount = request.args.get('row')
    if query is None:
      return 'No provided.', 400
    result = SL.sammarize(query, rowcount)
    if result is None:
      return 'not found : %s' % query, 400
    return result, 200


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
