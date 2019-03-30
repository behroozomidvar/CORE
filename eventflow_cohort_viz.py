# format of eventflow input
# idpatient -- label -- from -- to -- ("--" means tab)

import psycopg2

# query = "select id_patient, label, from_date, to_date from treatments where type = 'pec' and id_patient in (select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=FALSE and birth_year > 1970)"
# query = "select id_patient, label, from_date, to_date from treatments where type = 'pec' and id_patient in ('96095','14892','66813','66210')"
# query = "select id_patient, label, from_date, to_date from treatments where type = 'pec' and id_patient in (select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=FALSE and birth_year > 1970 and id_patient not in ('96095','14892','66813','66210', '43050','18916','10658','87498', '97110', '30037', '68651', '94890', '37219', '17682', '68049', '97599', '14996', '87037', '58620', '28440'))"
# query = "select id_patient, label, from_date, to_date from treatments where type = 'pec' and id_patient in (select id_patient from patients where gender = 'F' and zipcode = '38000' and dead=True and birth_year > 1950)"
# query = "select id_patient, label, from_date, to_date from treatments where type = 'pec' and id_patient in (select id_patient from patients where region = 'Ile-de-France ');"
# query = "select id_patient, label, from_date, to_date from treatments where type = 'pec' and id_patient in (select id_patient from patients where city like '%TOULOUSE%' and dead=True);"

query = "select id_patient, label, from_date, to_date from actions where type = 'pec' and id_patient in (select id_patient from patients where zipcode = '38000' and dead='True') order by id_patient,from_date;"


conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

cur.execute(query)
rows = cur.fetchall()

for row in rows:
	id_patient = row[0]
	label = row[1]
	from_date = row[2]
	to_date = row[3]
	if from_date != None and to_date != None:
		print str(id_patient)+"\t"+label+"\t"+str(from_date)+"\t"+str(to_date)+"\t "