This folder store temporary files (pickles or sqlite3) that store fequenqly used queries
the name of the cache file should include the time stamp to be able to invalidate it in case is stale without querying the file

The methods that retireve the data will query first the cache and if is stale it will create a new cache file
there is a background process that clean up the stale files in the cache

these file should not be included in version control