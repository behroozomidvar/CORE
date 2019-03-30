import psycopg2
import sys
import trajectories

# database connection
conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

query = "select id_patient from patients"

cur.execute(query)
rows = cur.fetchall()

# f_out = open("treatments.dat","w")
f_out1 = open("markers.dat","w")

cnt_patient = 0
for row in rows:
	cnt_patient += 1
	if cnt_patient % 100 == 0:
		print cnt_patient
	# f_out.write(str(row[0])+ " " + str(trajectories.get_sorted_events(row[0]))+"\n")
	f_out1.write(str(row[0])+ " " + str(trajectories.get_sorted_markers(row[0]))+"\n")

