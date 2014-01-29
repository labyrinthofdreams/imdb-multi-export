Export IMDb ratings in CSV format

Create a CSV file without a header consisting of username,url lines, e.g.

foo,http://www.imdb.com/user/ur00000000/ratings
bar,http://www.imdb.com/user/ur00000001/ratings
baz,http://www.imdb.com/user/ur00000002/ratings

And run: python imdb_csv.py imdb_urls.csv archive\20140101

And the ratings are saved as foo.csv, bar.csv, and baz.csv.

Optional arguments:

--cookies cookies.txt
Save IMDb cookies in a text file in format key=value; key=value; [...]

--retries 100
How many attempts to download failed exports.

--overwrite
If specified, will re-download all lists even if they exist in the
output directory. Otherwise only missing ratings are downloaded.

--threads 3
Specify how many simultaneous downloads you want to have running. 