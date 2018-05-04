# coding=utf-8
import csv
import calendar
import datetime
import json
import random

import itertools
import requests

from bs4 import BeautifulSoup
from janome.tokenizer import Tokenizer
from requests_oauthlib import OAuth1Session
from summpy.lexrank import summarize
from google.cloud import bigquery
from google.oauth2 import service_account


# twitterAPI
oath_key_dict = {
    "consumer_key": "2qimKikZwCOJXG0wxJ0lzkcM6",
    "consumer_secret": "MHAjJsYvGCF0mVkgs9w0tJh0fJf0ZpBMKqUMiqTUzQmqYoIFA2",
    "access_token": "157729228-r5JXs6Mi79rEgPAd1AyS9w5l7BaUADzrmLpc9JiR",
    "access_token_secret": "Dm0C0ZPCBCDcNARnAaJvUDxEk88o1pbTtWuZgvILzFG2u"
}

research_ids = ["get2ch_soccer", "BaseballNEXT", "gorin"]
pattern = r"(https?|ftp)(:\/\/[-_\.!~*\'()a-zA-Z0-9;\/?:\@&=\+\$,%#]+)"
rss_news = [r"https://headlines.yahoo.co.jp/rss/jsportsv-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/soccerk-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/bfj-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/nallabout-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/asahik-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/baseballk-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/spnaviv-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/tennisnet-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/nksports-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/gekisaka-c_spo.xml",
            r"https://headlines.yahoo.co.jp/rss/fullcount-c_spo.xml"]
json_key = 'Sports-Agent-f6e6a0a6dbc3.json'
base_url = [r"http://www.baseball-lab.jp"]
player_url = [r"/player/batter/",
              r"/player/pitcher/"]
team_url = [r'6/',
            r'5/',
            r'3/',
            r'1/',
            r'4/',
            r'2/',
            r'12/',
            r'7/',
            r'376/',
            r'11/',
            r'8/',
            r'9/']

player_record = {}
months = {}
for i, v in enumerate(calendar.month_abbr):
    months[v] = i


def create_oath_session(oath_key_dict):
    oath = OAuth1Session(
        oath_key_dict["consumer_key"],
        oath_key_dict["consumer_secret"],
        oath_key_dict["access_token"],
        oath_key_dict["access_token_secret"]
    )
    return oath


class SportsLive:
    def __init__(self, parent=None):
        pass

    '''
    形態素解析
    '''
    @staticmethod
    def morphological_analysis(text):
        txt = text
        t = Tokenizer()
        word_dic = {}
        lines = txt.split("\r\n")
        for line in lines:
            blog_txt = t.tokenize(line)
            for w in blog_txt:
                word = w.surface
                ps = w.part_of_speech
                if ps.find('名詞') < 0:
                    continue
                if word not in word_dic:
                    word_dic[word] = 0
                word_dic[word] += 1

        keys = sorted(word_dic.items(), key=lambda x: x[1], reverse=True)
        keyword = ''
        for word, cnt in keys[:4]:
            print("{0} ".format(word))
            keyword += "{0} ".format(word)

        return keyword

    def score_check(self, keyword):
        data = []

        try:
            target_url = 'https://sports.yahoo.co.jp/search/text?query=' + keyword
            resp = requests.get(target_url)
            soup = BeautifulSoup(resp.text, "html.parser")

            tables = soup.find_all("p", class_="siteUrl")

            for table in tables:
                geturl = table.text
                geturl = geturl.rstrip(' － キャッシュ')

                data.append(geturl)
        except:
            pass
        score = ''

        try:
            for url in data:
                if 'game' in url:
                    score = self.get_score(url)
                    break
                else:
                    continue

        except:
            pass

        return score

    def twitter_check(self, keyword, debug=False):
        keyword_list = keyword.split(' ')
        tweet_list = []
        output_list = []
        json_dict = {}

        for keyword in keyword_list:
            if keyword == "":
                break

            for research_id in research_ids:
                tweets = self.tweet_search(keyword, oath_key_dict, research_id)

                for tweet in tweets["statuses"]:
                    text = tweet['text']
                    text = self.tweet_analysis(text)
                    if not text[0] in outtext:
                        outtext += text[0] + '<br>'

                outtext2 += outtext[:600]
                outtext = ''

            outtext2 = outtext2.replace(keyword, '<font color="red">' + keyword + '</font>')

        return outtext2

    def news_check(self, keyword, debug=False):
        news_dict = {}
        keyword = keyword.split(' ')
        output_text = ""
        json_dict = {}

        for rss in rss_news:
            resp = requests.get(rss)
            soup = BeautifulSoup(resp.text, "html.parser")

            titles = soup.find_all("title")
            links = soup.find_all("link")

            for title, link in zip(titles, links):
                news_dict.update({title.next: str(link.next).replace('\n', '').replace(' ', '')})

        for key in keyword:
            if key == "":
                break

            news_key_list = [l for l in news_dict.keys() if key in l]

            for list_key in news_key_list:
                text = ""
                resp = requests.get(news_dict[list_key])
                soup = BeautifulSoup(resp.text, "html.parser")

                for s in soup.find_all("p", class_="ynDetailText"):
                    text += s.get_text()
                analysis_text = self.tweet_analysis(text)

                if debug:
                    # タイトル：｛リンク，全文，要約｝
                    json_dict.update({list_key:
                    {
                        'link':news_dict[list_key],
                        'text':text,
                        'a_text':analysis_text,
                    }}
                    )

                output_text += '<br>'.join(analysis_text)

        json_dict.update({"result_text":output_text})

        encode_json_data = json.dumps(json_dict)

        return encode_json_data

    @staticmethod
    def news_loader(keyword, rowcount, day, debug=False):
        news_dict = {}
        # keyword = keyword.split(' ')
        output_text = ""
        json_dict = {}
        config = bigquery.QueryJobConfig()
        config.use_legacy_sql = True

        try:
            if 1 <= rowcount < 5:
                rowcount_str = "row{}_text".format(str(rowcount))
            else:
                rowcount_str = "Full_text"
        except:
            NameError("test error")

        if debug AND rowcount_str == "Full_text":
            myquery = """
                        SELECT title,Full_text as text
                        FROM sportsagent.newsrecord
                        WHERE title like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(day, str(keyword))
        if debug:
            myquery = """
                        SELECT title,Full_text,{0} as text
                        FROM sportsagent.newsrecord
                        WHERE title like '%{2}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(rowcount_str, day, str(keyword))
        else:
            myquery = """
                        SELECT {0} as text
                        FROM sportsagent.newsrecord
                        WHERE title like '%{2}%' AND _PARTITIONTIME = TIMESTAMP('{1}')
                      """.format(rowcount_str, day, str(keyword))
        try:
            client = bigquery.Client.from_service_account_json(json_key, project='sports-agent-199307')
            query_job = client.query(myquery, job_config=config)
            results = query_job.result()  # Waits for job to complete.
        except:
            raise NameError(myquery)
        
        try:
            if 1 <= rowcount < 5:
                # random select for results
                randindex = random.randint(0, len(results) - 1)
                output_text = results[randindex].text
            else:
                text = "".join([re.text for re in results])
                output_text = self.analsys_text(text, rowcount)
        except:
            raise NameError("get errors?")
            
        if debug:
            for result in results:
                json_dict.update({result.title:
                {
                    'text':result.Full_text,
                    'a_text':result.text
                }}
                )

        json_dict.update({"result_text":output_text})

        encode_json_data = json.dumps(json_dict)

        return encode_json_data

    def player_loader(self, keyword, day, debug=False):
        news_dict = {}
        keyword = keyword.split(' ')
        output_text = ""
        json_dict = {}
        client = bigquery.Client.from_service_account_json(json_key, project='sports-agent-199307')
        config = bigquery.QueryJobConfig()
        config.use_legacy_sql = True

        if debug:
            myquery = """
                        SELECT name,record as text
                        FROM sportsagent.playerrecord
                        WHERE name like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                      """.format(day, str(keyword))
        else:
            myquery = """
                        SELECT name,record as text
                        FROM sportsagent.playerrecord
                        WHERE name like '%{1}%' AND _PARTITIONTIME = TIMESTAMP('{0}')
                      """.format(day, str(keyword))

        query_job = client.query(myquery, job_config=config)
        results = query_job.result()  # Waits for job to complete.

        output_text = "".join([re.text for re in results])

        if debug:
            for result in results:
                json_dict.update({result.title:
                {
                    'text':result.Full_text,
                    'a_text':result.text
                }}
                )

        json_dict.update({"result_text":output_text})

        encode_json_data = json.dumps(json_dict)

        return encode_json_data

    @staticmethod
    def tweet_search(search_word, oath_key_dict, account):
        url = "https://api.twitter.com/1.1/search/tweets.json?"
        params = {
            "q": search_word,
            "from":account,
            "lang": "ja",
            "result_type": "recent",
            "count": "100"
        }

        oath = create_oath_session(oath_key_dict)
        responce = oath.get(url, params=params)
        if responce.status_code != 200:
            print("Error code: %d" % (responce.status_code))
            return None
        tweets = json.loads(responce.text)

        return tweets

    @staticmethod
    def get_score(url):
        target_url = url
        resp = requests.get(target_url)
        soup = BeautifulSoup(resp.text)

        if 'baseball' in url:
            score_table = soup.find('table', {'width': "100%", 'cellpadding': "0", 'cellspacing': "0", 'border': "0"})
            rows = score_table.findAll("tr")
            score = []
            text = '最新の試合の結果は' + '\n'

            try:
                for row in rows:
                    csvRow = []
                    for cell in row.findAll(['td', 'th']):
                        csvRow.append(cell.get_text())
                    score.append(csvRow)

                    text += '\t|'.join(csvRow) + '\n'

            finally:
                return text

        elif 'soccer' in url:
            hometeam = soup.find_all('div', class_="homeTeam team")
            hometotal = soup.find_all("td", class_="home goal")
            home1st = soup.find_all("td", class_="home first")
            home2nd = soup.find_all("td", class_="home second")
            awayteam = soup.find_all('div', class_="awayTeam team")
            awaytotal = soup.find_all("td", class_="away goal")
            away1st = soup.find_all("td", class_="away first")
            away2nd = soup.find_all("td", class_="away second")

            for homename, awayname, homegoal, awaygoal in zip(hometeam, awayteam, hometotal, awaytotal):
                text = '最新の試合の結果は' + '\n' + str(homename.text.replace('\n', '')) + \
                       '-' + str(awayname.text.replace('\n', '')) + '\n'

                if len(home1st[0].text) > -1:
                    text += home1st[0].text + '前半' + away1st[0].text + '\n'

                if len(home2nd[0].text) > -1:
                    text += home2nd[0].text + '後半' + away2nd[0].text + '\n'

                if len(homegoal) > -1:
                    text += homegoal.text + ' - ' + awaygoal.text

                return text

    @staticmethod
    def tweet_analysis(text):
        sentences, debug_info = summarize(
            text, sent_limit=5, continuous=True, debug=True
        )

        return sentences

    @staticmethod
    def analsys_text(text, rowcount):
        sentences, debug_info = summarize(
            text, sent_limit=rowcount, continuous=True, debug=True
        )

        return sentences

    @staticmethod
    def sammarize(text, rowcount):
        json_dict = {}
        sentences, debug_info = summarize(
            text, sent_limit=rowcount, continuous=True, debug=True
        )
        
        output_text = " ".join(sentences)
        json_dict.update({"result_text": output_text})
        encode_json_data = json.dumps(json_dict)

        return encode_json_data


class RecordAccumulation:
    def __init__(self):
        pass

    @staticmethod
    def get_player_dic(date):
        year = date.year
        month = date.month
        day = date.day

        dic = {}
        for path in list(itertools.product(base_url, player_url, team_url)):
            req = requests.get("".join(path) + str(year) + "/")

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            table = soup.findAll("table", class_="tbl-stats")[0]
            tbody = table.findAll("tbody")[0]
            td = tbody.findAll("td", class_="t-name")

            for data in td:
                a_tag = data.find("a")

                dic.update({a_tag.text: [base_url[0] + a_tag.attrs["href"], path[1]]})

        return dic

    def get_player_record(self, player_dic, date):
        rec_list = [["name", "player_type", "record"]]
        rec_tuple = []

        for key in player_dic.keys():
            req = requests.get(player_dic[key][0])

            try:
                soup = BeautifulSoup(req.text, "lxml")
            except:
                soup = BeautifulSoup(req.text, "html.parser")

            try:
                div = soup.findAll("div", class_="box", id="player-game-logs")[0]

                if "batter" in player_dic[key][1]:
                    table = div.findAll("table", class_="tbl-stats tbl-stats-batting")[0]
                    rec = [str(key)] + self.get_record(table, "b", date)
                    rec_list.append(rec)
                    rec_tuple.append(tuple(rec))
                else:
                    table = div.findAll("table", class_="tbl-stats tbl-stats-pitching")[0]
                    rec = [str(key)] + self.get_record(table, "p", date)
                    rec_list.append(rec)
                    rec_tuple.append(tuple(rec))
                    table_bat = div.findAll("table", class_="tbl-stats tbl-stats-batting")[0]
                    rec = [str(key)] + self.get_record(table_bat, "b", date)
                    rec_list.append(rec)
                    rec_tuple.append(tuple(rec))
            except:
                continue

        return rec_list, rec_tuple

    @staticmethod
    def get_record(table, player_type, date):
        month = date.month
        day = date.day
        tbody = table.findAll("tbody")[0]
        tr_list = tbody.findAll("tr")
        record = ""

        for tr in tr_list:
            td = tr.findAll("td")

            if "{0}/{1}".format(month, day) in td[0].text:
                if player_type == "b":
                    record = td[3].text + td[3].nextSibling \
                             + td[5].text + td[5].nextSibling \
                             + " " + td[4].text + td[4].nextSibling \
                             + td[21].nextSibling + ":" + td[21].text
                else:
                    record = td[4].text + "回から" \
                             + td[5].text + "回まで投げて" \
                             + td[6].text + td[6].nextSibling + "に対して" \
                             + td[11].text + td[11].nextSibling \
                             + td[7].text + td[7].nextSibling
                    if "●" in td[2].text:
                        record += "で、負けました。"
                    else:
                        record += "で、勝ちました。"
                break
            else:
                pass
        if record is "":
            raise Exception

        record = record.replace("\t", "").replace("  ", "")

        return [str(player_type), str(record)]

    @staticmethod
    def save_csv(table, filename):
        with open(filename, "w", encoding="utf-8", newline='') as f:
            writer = csv.writer(f)
            for row in table:
                writer.writerow(row)

    def news_check(self, date):
        news_dict = {}
        output_text = ""
        news_list = [["title", "url", "Full_text", "row1_text", "row2_text", "row3_text", "row4_text"]]
        news_tuple = []

        for rss in rss_news:
            resp = requests.get(rss)
            soup = BeautifulSoup(resp.text, "html.parser")

            items = soup.find_all("item")

            for item in items:
                title = item.find_all("title")[0]
                link = item.find_all("link")[0]
                day = item.find_all("pubdate")[0].text

                news_date = day.split(" ")
                news_date = datetime.date(int(news_date[3]),
                                          int(months[news_date[2]]),
                                          int(news_date[1]))
                if date == news_date:
                    news_dict.update({title.text: str(link.next).replace('\n', '').replace(' ', '')})

        news_key_list = [l for l in news_dict.keys()]

        for list_key in news_key_list:
            news = [str(list_key), str(news_dict[list_key])]
            text = ""
            resp = requests.get(news_dict[list_key])
            soup = BeautifulSoup(resp.text, "html.parser")

            for s in soup.find_all("p", class_="ynDetailText"):
                text += s.get_text()

            news.append(text)
            for r_count in range(1, 5):
                analysis_text = self.sammarize(text, r_count)
                output_text = ''.join(analysis_text)
                news.append(str(output_text))

            news_list.append(news)
            tnews = tuple(news)
            news_tuple.append(tnews)

        return news_list, news_tuple

    @staticmethod
    def sammarize(text, rowcount):
        try:
            sentences, debug_info = summarize(
                text, sent_limit=rowcount, continuous=True, debug=True
            )
        except:
            sentences = "sammarized error"

        return sentences


def main():
    # SL = SportsLive()

    # print(SL.news_check(SL.morphological_analysis('羽生のオリンピック')))
    # print(SL.news_check(SL.morphological_analysis('宇野昌磨の記録'), debug=True))

    RA = RecordAccumulation()
    today = datetime.date(2018, 4, 18)
    test = RA.news_check(today)
    # test = RA.get_player_dic(today)
    # table = RA.get_player_record(test, today)
    RA.save_csv(test, "record.csv")


if __name__ == '__main__':
    main()

