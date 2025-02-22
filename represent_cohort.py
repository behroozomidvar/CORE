# NEW VERSION OF THE CODE FOR VLDB JOURNAL
# UPDATE ON MARCH AND APRIL 2019

import psycopg2
import sys
import trajectories
import sequence_matching
import core_functions
import time
import events
import random

# CONFIGURATION PARAMETERS - BEGIN
random_patients = True # just for experiments -- It ignores reading input parameters and randomize patients.
nb_random_patients = 50
show_representation = True
enable_clustering = False
enable_sampling = False
nb_clusters = 50
sampling_ratio = 0.2 # a value between 0 and 1, "1" means no sampling
attribute_for_stratified_sampling = 'random' # should be one of the followings "age", "gender", "life" -- it can also be "random" to turn stratified sampling to random sampling.
significance_threshold = 0.01
dataset_name = 'r' # either 'a' (agir) or 'r' (rambam)
demographics_line = "F,40,*,*"
# actions_line = "'pec OXY E','pec FIT B'"
actions_line = "*"
# CONFIGURATION PARAMETERS - END

# SETTING COHORT DEMOGRAPHICS - BEGIN
demographics = demographics_line.split(",")
gender = demographics[0]
age = demographics[1]
location = demographics[2]
life = demographics[3]
# SETTING COHORT DEMOGRAPHICS - END

# DEFINITION OF CONSTANTS - BEGIN
dataset_size = {'a': 56286, 'r': 260099}
cohort_members = []
# DEFINITION OF CONSTANTS - END

# DB CONNECTION - BEGIN
conn = psycopg2.connect("dbname='core' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()
# DB CONNECTION - END

# CLUSTERING - BEGIN
if enable_clustering == True:
	nb_random_patients = core_functions.nb_patients_for_clustering(nb_clusters,nb_random_patients)
# CLUSTERING - END

# STRATIFIED SAMPLING - BEGIN
if enable_sampling == True:
	if attribute_for_stratified_sampling != "random":
		cohort_members = core_functions.stratified_sampling(sampling_ratio,attribute_for_stratified_sampling,nb_random_patients,dataset_name)
	else:
		nb_random_patients *= sampling_ratio
		enable_sampling = False
# STRATIFIED SAMPLING - END

# FIND COHORT MEMBERS - BEGIN
start = time.time()
if enable_sampling == False:
	cohort_members_query = ""
	if random_patients == True:
		cohort_members_query = "select patient_id from patients where dataset='"+dataset_name+"' order by random() limit "+str(nb_random_patients)+";"
	else:
		cohort_members_query = "select distinct(p.patient_id) from patients p, events e where p.patient_id=e.patient_id and p.dataset='"+dataset_name+"' and e.dataset = '"+dataset_name+"' "
		if gender != "*":
			cohort_members_query += "and p.gender='"+gender+"' "
		if age != "*":
			cohort_members_query += "and p.age ="+str(age)+" "
		if life != "*":
			cohort_members_query += "and p.life = '"+life+"' "
		if dataset_name == "a" and location != "*":
			cohort_members_query += "and p.location = '"+location+"' "
		if actions_line != "*":
			cohort_members_query += "and e.action_id in (select action_id from actions where name in ("+actions_line+"));"
	cur.execute(cohort_members_query)
	rows = cur.fetchall()
	for row in rows:
		cohort_members.append(row[0])
	end = time. time()
	dur = round((end - start)*1000,2)
	print "found "+str(len(cohort_members))+" cohort members in "+str(dur)+" ms."
	# print str(dur)+" ms."
	if len(cohort_members) == 0:
		print "there is a problem!"
		exit(1)
# print "there are "+str(len(cohort_members))+" cohort members."
# FIND COHORT MEMBERS - END

# FIND COHORT TRAJECTORIES - BEGIN
start = time.time()
# print "collecting patient trajectories ..."
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
end = time. time()
dur = round((end - start)*1000,2)
# print "collected trajectories in "+str(dur)+" ms."
print "trajectory extraction: "+str(dur)+" ms."
# FIND COHORT TRAJECTORIES - END

# FIND ALL AGGREGATED EVENTS IN THE COHORT - BEGIN
start = time.time()
# print "computing all aggregated events in the cohort ..."
all_aggregated_events_of_the_cohort = []
for patient1 in cohort_members:
	for patient2 in cohort_members:
		if patient1 == patient2:
			continue
		trajectory1 = trajectories_of[patient1]
		trajectory2 = trajectories_of[patient2]
		common_actions_between_trajectories = core_functions.common_actions(trajectory1,trajectory2)
		for common_action_between_trajectories in common_actions_between_trajectories:
			# max_length = float(max(length_of_trajectory[patient1],length_of_trajectory[patient2]))
			# cost = round((max_length - (max(ccc.psi) - min(ccc.psi)))/max_length,2)
			aggregated_event = events.AggregatedEvent([patient1,patient2],common_action_between_trajectories.action, common_action_between_trajectories.psi)
			all_aggregated_events_of_the_cohort.append(aggregated_event)
end = time. time()
dur = round((end - start)*1000,2)
# print "computed aggregated events in "+str(dur)+" ms."
print "compute aggregated events: "+str(dur)+" ms."
# FIND ALL AGGREGATED EVENTS IN THE COHORT - END

# BULDING REPRESENTATIONS - BEGIN
start = time.time()
# print "building representations ..."
my_buffer = []
actions_in_the_buffer = []
for aggregated_event in all_aggregated_events_of_the_cohort:
	if aggregated_event.action in actions_in_the_buffer:
		for element in my_buffer:
			if element.action == aggregated_event.action:
				element.members = element.members + aggregated_event.members
				element.psi = element.psi + aggregated_event.psi
				break
	else:
		actions_in_the_buffer.append(aggregated_event.action)
		my_buffer.append(aggregated_event)

cohort_representation = []
max_significance = 0
for element in my_buffer:
	significance = len(element.psi) / 2
	dispersion = max(element.psi) - min(element.psi)
	cohort_representation.append(events.RepresentationEvent(element.action,significance,dispersion))
	if significance > max_significance:
		max_significance = significance
end = time. time()
dur = round((end - start)*1000,2)
# print "built representation in "+str(dur)+" ms."
print "representation fromation: "+str(dur)+" ms."
# APPLYING SIGNIFICANCE THRESHOLD - END

# NORMALIZE SIGNIFICANCE IN COHORT REPRESENTATION - BEGIN
start = time. time()
for e in cohort_representation:
	e.significance /= float(max_significance)
end = time. time()
dur = round((end - start)*1000,2)
# print "normalized significance in "+str(dur)+" ms."
# NORMALIZE SIGNIFICANCE IN COHORT REPRESENTATION - END

pruned_representation = []
for e in cohort_representation:
	if e.significance > significance_threshold:
		pruned_representation.append(e)

if show_representation == True:
	print pruned_representation

# MEASURE QUALITY OF REPRESENTATION - BEGIN
# start = time.time()
# fit = core_functions.get_fit(pruned_representation,trajectories_of)
# print "fit", fit
# coverage = core_functions.get_coverage(pruned_representation,trajectories_of)
# print "coverage", coverage
# generality = core_functions.get_generality(pruned_representation,dataset_name,dataset_size[dataset_name])
# print "generality", generality
# print "measured the quality of the representation in "+str(dur)+" ms."
# MEASURE QUALITY OF REPRESENTATION - END
