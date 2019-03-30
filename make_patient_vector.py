import psycopg2
import trajectories

# output files
f_vector = open("vector.dat","w")
f_patients = open("patients.dat","w")
f_treatments = open("treatments.dat","w")

# database connection
conn = psycopg2.connect("dbname='health_trajectory' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

treatments = {}
treatment_cnt = 0
patients = {}
patient_cnt = 0

query_treatments = "select distinct label from actions where type='pec'"
query_patients = "select distinct id_patient from patients"

cur.execute(query_treatments)
rows = cur.fetchall()

for row in rows:
	treatments[treatment_cnt] = row[0]
	f_treatments.write(str(row[0])+"\n")
	treatment_cnt += 1

f_treatments.close()

cur.execute(query_patients)
rows = cur.fetchall()

for row in rows:
	patients[patient_cnt] = row[0]
	f_patients.write(str(row[0])+"\n")
	patient_cnt += 1

f_patients.close()

for i in range(0,patient_cnt):
	vector = ""
	patient_actions = []
	treatment_durations = {}
	query_vector = "select distinct label, from_date, to_date from actions where type = 'pec' and id_patient = '"+str(patients[i])+"';"
	cur.execute(query_vector)
	rows = cur.fetchall()
	for row in rows:
		duration = trajectories.month_diff(str(row[1]),str(row[2]))
		if row[0] in patient_actions:
			treatment_durations[row[0]] += duration
		else:
			patient_actions.append(row[0])
			treatment_durations[row[0]] = duration

	for j in range(0,treatment_cnt):
		if treatments[j] in patient_actions:
			f_vector.write(str(treatment_durations[treatments[j]])+ " ")
		else:
			f_vector.write("0 ")
	f_vector.write("\n")
	if i % 100 == 0:
		print i



