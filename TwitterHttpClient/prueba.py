url = 'https://stream.twitter.com/1.1/statuses/filter.json'
#query_data = [('language', 'en'), ('locations', '-74,40,-73,41'),('track','#')]
query_data = [('track', '#')]
query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
print(query_url)