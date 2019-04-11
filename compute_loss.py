import psycopg2
import sys
import trajectories
import sequence_matching
import core_functions
import time
import events
import random

nb_random_patients = 200
reduced_nb_patients = nb_random_patients
enable_clustering = True
enable_sampling = False
nb_clusters = 200
sampling_ratio = 0.2 # a value between 0 and 1, "1" means no sampling
attribute_for_stratified_sampling = 'random' # should be one of the followings "age", "gender", "life" -- it can also be "random" to turn stratified sampling to random sampling.
dataset_name = 'r' # either 'a' (agir) or 'r' (rambam)

conn = psycopg2.connect("dbname='core' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

if enable_clustering == True:
	reduced_nb_patients = core_functions.nb_patients_for_clustering(nb_clusters,nb_random_patients)

if enable_sampling == True:
	reduced_nb_patients = nb_random_patients * sampling_ratio

# for nb_clusters in (10,50,100,150,200):
cohort_members_query = "select patient_id from patients where dataset='"+dataset_name+"' order by random() limit "+str(nb_random_patients)+";"

cohort_members = []
cur.execute(cohort_members_query)
rows = cur.fetchall()
for row in rows:
	cohort_members.append(row[0])

reduced_cohort_members = []
while len(reduced_cohort_members) < reduced_nb_patients:
	random_patient_index = random.randint(0,nb_random_patients-1)
	random_patient = cohort_members[random_patient_index]
	if random_patient not in reduced_cohort_members:
		reduced_cohort_members.append(random_patient)

unpicked_cohort_members = []
for patient in cohort_members:
	if patient not in reduced_cohort_members:
		unpicked_cohort_members.append(patient)

trajectories_of = {}
length_of_trajectory = {}
for patient in cohort_members:
	trajectories_of[patient] = []
	length_of_trajectory[patient] = 0
trjactory_query = "select e.patient_id, c.name, e.time from events e, actions c where e.action_id = c.action_id and e.dataset = '"+dataset_name+"' and c.dataset= '"+dataset_name+"' and e.patient_id in ("+core_functions.members_concat(cohort_members)+") order by e.patient_id, e.time asc"
cur.execute(trjactory_query)
rows = cur.fetchall()
for row in rows:
	patient_id = row[0]
	this_action = row[1]
	this_time = row[2]
	trajectories_of[patient_id].append(events.Event(patient_id,this_action,this_time))
	if this_time > length_of_trajectory[patient_id]:
		length_of_trajectory[patient_id] = this_time

cnt_cmp = 0
sum_dis = 0
for patient1 in unpicked_cohort_members:
	for patient2 in reduced_cohort_members:
		for event1 in trajectories_of[patient1]:
			for event2 in trajectories_of[patient2]:
				if event1.action == event2.action:
					sum_dis += abs(event1.time - event2.time) / float(max(length_of_trajectory[patient1],length_of_trajectory[patient2]))
					cnt_cmp += 1

print round(float(sum_dis) / cnt_cmp, 2)