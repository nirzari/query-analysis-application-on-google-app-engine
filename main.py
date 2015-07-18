#NAME: NIRZARI IYER
#Assignment-2
#ID NUMBER: 1001117633
#BATCH TIME- 6:00 to 8:00 p.m.
import MySQLdb
import io
import os
import cloudstorage as gcs
import csv
import timeit
from bottle import Bottle
from google.appengine.api import app_identity
from StringIO import StringIO
from bottle import route, request, response, template

bottle = Bottle()

#location of file into default bucket on google cloud storage
bucket_name = os.environ.get('BUCKET_NAME', app_identity.get_default_gcs_bucket_name())
bucket = '/' + bucket_name
filename = bucket + '/earthquake.csv'

#Get filename from user
@bottle.route('/uploadform')
def uploadform():
    return template('upload_form')

#Upload file into bucket on google cloud storage
@bottle.route('/uploadfile',  method='POST')
def uploadfile():
    start = timeit.default_timer()
    filecontent = request.files.get('filecontent')
    rawfilecontent = filecontent.file.read()
    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,'w',content_type='text/plain',retry_params=write_retry_params)
    gcs_file.write(rawfilecontent)
    gcs_file.close()
    stop = timeit.default_timer()
    time_taken = stop - start
    return template('upload_file',time_taken=time_taken)

#Read data from bucket and Insert data into google MySQLdb
def parse(filename, delimiter,c):
    with gcs.open(filename, 'r') as gcs_file:
        csv_reader = csv.reader(StringIO(gcs_file.read()), delimiter=',',
                     quotechar='"')
	# Skip the header line
        csv_reader.next()   
        try:
	    start = timeit.default_timer()
            for row in csv_reader:
	        time = timestamp(row[0])
		updated = timestamp(row[12])
                for i in range (0,14):
		    if row[i] == '':
                        row[i] = "''"
		place = str(row[13])
		place = place.replace("'","")
                insert = "INSERT INTO earthquake (time, latitude, longitude, depth, mag, magType, nst, gap, dmin, rms, net, id, updated,\
                         place, type) values('"+time+"',"+row[1]+","+row[2]+","+row[3]+","+row[4]+",'"+row[5]+"',"+row[6]+","+row[7]+",\
                         "+row[8]+","+row[9]+",'"+row[10]+"','"+row[11]+"','"+updated+"','"+place+"','"+row[14]+"')"
                c.execute(insert) 
	    stop = timeit.default_timer()
            insert_time = stop - start	    
            return insert_time

        except Exception as e:
            print ("Data can't be inserted" + str(e))

def timestamp(string):
    ans = string[:10] + ' ' + string[11:19]
    return ans

def query(mag,c):
    query = 'SELECT week(time) as week, count(*) as count, mag as mag FROM earthquake WHERE mag = '+str(mag)+' GROUP BY week(time), mag'
    c.execute(query)
    ans_query = c.fetchall()
    return ans_query

def bigquery(mag,c):
    query = 'SELECT week(time) as week, count(*) as count, mag as mag FROM earthquake WHERE mag > '+str(mag)+' GROUP BY week(time), mag'
    c.execute(query)
    ans_query = c.fetchall()
    return ans_query

def ans_format(mag):
    table = "<table border='2'><tr><th>Week</th><th>Number of quakes</th><th>Magnitude</th></tr>"
    ans = ""
    for x in mag:
        ans = ans +"<tr><td>" + str(x[0]) + "</td><td>" + str(x[1]) + "</td><td>" + str(x[2]) +"</td></tr>"
    table += ans + "</table>"
    return table	
  
@bottle.route('/')
def main():
    try:
        connobj = MySQLdb.connect(unix_socket='/cloudsql/cloudcomp2-979:simple' ,user='root')
        c = connobj.cursor()
        createdb = 'CREATE DATABASE IF NOT EXISTS db'
        c.execute(createdb)
        connectdb = 'USE db'
        c.execute(connectdb)
        table = 'CREATE TABLE IF NOT EXISTS earthquake '\
                '(time TIMESTAMP,'\
                'latitude DOUBLE,'\
                'longitude DOUBLE,'\
                'depth DOUBLE,'\
                'mag DOUBLE,'\
                'magType varchar(500),'\
                'nst DOUBLE,'\
                'gap DOUBLE,'\
                'dmin DOUBLE,'\
                'rms DOUBLE,'\
                'net varchar(500),'\
                'id varchar(500),'\
                'updated TIMESTAMP,'\
                'place VARCHAR(500),'\
                'type VARCHAR(500))'
        c.execute(table)
        c.execute("truncate table earthquake")
        insert_time = parse(filename,',',c)
        mag2 = query(2,c) 
	mag3 = query(3,c)
	mag4 = query(4,c)
	mag5 = query(5,c)
	maggt5 = bigquery(5,c)
	ans_mag2 = ans_format(mag2)
	ans_mag3 = ans_format(mag3)
	ans_mag4 = ans_format(mag4)
	ans_mag5 = ans_format(mag5)
	ans_maggt5 = ans_format(maggt5)  
        ans = "Final Result: <br><br> Time taken to Insert data into MySQL database is: <br>" +str(insert_time)+"<br><br>" \
	    "Earthquake of magnitude 2: <br> "+str(ans_mag2)+"<br><br> Earthquake of magnitude 3: <br>" \
            +str(ans_mag3)+ "<br><br> Earthquake of magnitude 4: <br>" +str(ans_mag4)+ "<br><br> Earthquake" \
	    "of magnitude 5: <br>" +str(ans_mag5)+ "<br><br> Earthquake of magnitude greater than 5: <br>" +str(ans_maggt5)
	return ans

    except Exception as e:
        print str(e)
        return e

# Define an handler for 404 errors.
@bottle.error(404)
def error_404(error):
    """Return a custom error 404."""
    return 'Sorry, nothing at this URL.'
# [END all]
