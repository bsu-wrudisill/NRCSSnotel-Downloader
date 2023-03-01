# NRCSSnotel-Downloader
This code will download NRCS SNOTEL (https://www.nrcs.usda.gov/wps/portal/wcc/home/) data from the web. The user provides the US State. All of the data will be saved in a sql data base file. I reccommend downloading a sql browser to look at the file once it's downloaded. https://sqlitebrowser.org/

The dependencies are fairly standard -- pandas being among them. You will need to also install the sqlalchemy (https://www.sqlalchemy.org/) dependency in order for pandas to read/write sql files. 

I think this is a fairly convenient way to store the data, rather than bunch of separate CSV files or a netcdf file. Actually doing analysis in a SQL framework is not really the point of this script (nor do I really know sql)... After creating the .db file, I always do analysis in python/pandas. I provide a sample .py script for reading a table from the database file as a pandas dataframe. 

Below is a screenshot of the database viewed with the "db browser for sqlite" program linked to above. There is a single table called "main" which has metadata from all of the stations. Then each station has a unique table assosciated with it, the name of which corresponds with the three or four digit station code.


![image](https://user-images.githubusercontent.com/19933988/222006868-7c460176-b460-475f-9a3c-5af74b60a676.png)


This is an exaple data table with SWE plotted.

![image](https://user-images.githubusercontent.com/19933988/222012445-3c053fea-9b57-4923-919b-88ecb778ef21.png)
