import events
import psycopg2

# DB CONNECTION - BEGIN
conn = psycopg2.connect("dbname='core' user='omidvarb' host='localhost' password='212799'")
cur = conn.cursor()
# DB CONNECTION - END

def age_category(age_str):
	age_categories = [0,18,24,34,44,49,55,150]
	age = int(age_str)
	for age_category in age_categories:
		if age < age_category:
			return age_category

def members_concat(members):
	final_str = ""
	for member in members:
		final_str += "'"+member+"',"
	final_str = final_str[:-1]
	return final_str

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

# REPRESENTATION QUALITY MEASURES - BEGIN
def get_fit(cohort_representation,trajectories_of):
	unhappy_patients = 0
	actions_in_the_cohort = []
	for e in cohort_representation:
		actions_in_the_cohort.append(e.action)
	for patient in trajectories_of:
		cnt = 0
		cnt_not = 0
		for e in trajectories_of[patient]:
			cnt += 1
			if e.action in actions_in_the_cohort:
				continue
			else:
				cnt_not += 1	
		if cnt_not == cnt:
			unhappy_patients += 1
	return 1.0 - round(float(unhappy_patients)/float(len(trajectories_of)),2)

def get_coverage(cohort_representation,trajectories_of):
	happy_patients = 0
	actions_in_the_cohort = []
	for e in cohort_representation:
		actions_in_the_cohort.append(e.action)
	for patient in trajectories_of:
		flag = True
		for e in trajectories_of[patient]:
			if e.action not in actions_in_the_cohort:
				flag = False
				break	
		if flag == True:
			happy_patients += 1
	return round(float(happy_patients)/float(len(trajectories_of)),2)

def get_generality(cohort_representation,dataset,nb_all_patients):
	actions_in_the_cohort = ""
	for e in cohort_representation:
		actions_in_the_cohort+="'"+e.action+"',"
	actions_in_the_cohort = actions_in_the_cohort[:-1]
	generality_query = "select patient_id from events where action_id in (select action_id from actions where name in ("+actions_in_the_cohort+")) and dataset = '"+dataset+"' group by patient_id having count(*) = (select count(*) from actions where name in ("+actions_in_the_cohort+"));"
	cur.execute(generality_query)
	rows = cur.fetchall()
	return round(float(len(rows)) / float(nb_all_patients),2)


# REPRESENTATION QUALITY MEASURES - END

