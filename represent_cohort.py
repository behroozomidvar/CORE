# NEW VERSION OF THE CODE FOR VLDB JOURNAL
# UPDATE ON MARCH AND APRIL 2019

import psycopg2
import sys
import cohorts
import trajectories
import sequence_matching
import core_functions
import time
import events

def common_actions(traj1, traj2):
	common = []
	for event1 in traj1:
		for event2 in traj2:
			if event1.action == event2.action:
				members = [event1.patient, event2.patient]
				action = event1.action
				psi = [event1.time, event2.time]
				aggregated_event = events.AggregatedEvent(members,action,psi)
				flag = False
				for entity in common:
					if entity.action == aggregated_event.action:
						flag = True
						if event1.time >= min(entity.psi) and event2.time <= min(entity.psi):
							entity.psi = psi
							break
				if flag == False:
					common.append(aggregated_event)
	return common

conn = psycopg2.connect("dbname='core' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()

# READ INPUT PARAMETERS - BEGIN
start = time.time()
input_parameters_file = open("input.dat","r")
dataset_line = input_parameters_file.readline() # either "agir" or "rambam"
demographics_line = input_parameters_file.readline()
actions_line = input_parameters_file.readline()
significance_threshold = float(input_parameters_file.readline())

demographics_line = demographics_line.strip()
demographics = demographics_line.split(",")
gender = demographics[0]
age = demographics[1]
location = demographics[2]
life = demographics[3]

actions_line = actions_line.strip()

dataset_name = dataset_line.strip()
access_path_to_dataset = dataset_name +"/"
end = time. time()
dur = round((end - start)*1000,2)
print "read parameters in "+str(dur)+" ms."
# READ INPUT PARAMETERS - END

# FIND COHORT MEMBERS - BEGIN
start = time.time()
# print "evaluating patients info against the input parameters ..."
cohort_members = []
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

print cohort_members_query
cur.execute(cohort_members_query)
rows = cur.fetchall()
for row in rows:
	cohort_members.append(row[0])
end = time. time()
dur = round((end - start)*1000,2)
print "found "+str(len(cohort_members))+" members for the cohort in "+str(dur)+" ms."
if len(cohort_members) == 0:
	print "there is a problem!"
	exit(1)
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
print "collected trajectories in "+str(dur)+" ms."
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
		common_actions_between_trajectories = common_actions(trajectory1,trajectory2)
		for common_action_between_trajectories in common_actions_between_trajectories:
			# max_length = float(max(length_of_trajectory[patient1],length_of_trajectory[patient2]))
			# cost = round((max_length - (max(ccc.psi) - min(ccc.psi)))/max_length,2)
			aggregated_event = events.AggregatedEvent([patient1,patient2],common_action_between_trajectories.action, common_action_between_trajectories.psi)
			all_aggregated_events_of_the_cohort.append(aggregated_event)
end = time. time()
dur = round((end - start)*1000,2)
print "computed all aggregated trajectories in "+str(dur)+" ms."
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
end = time. time()
dur = round((end - start)*1000,2)
print "built representation in "+str(dur)+" ms."
# BULDING REPRESENTATIONS - END

# APPLYING SIGNIFICANCE THRESHOLD - BEGIN
start = time.time()
# print "building representations ..."
for element in my_buffer:
	significance = len(element.psi) / 2
	dispersion = max(element.psi) - min(element.psi)
	if significance > significance_threshold:
		print element.action, "significance:", significance, "dispersion:", dispersion
end = time. time()
dur = round((end - start)*1000,2)
print "done in "+str(dur)+" ms."
# APPLYING SIGNIFICANCE THRESHOLD - END

