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

def nb_patients_for_clustering(nb_clusters,nb_random_patients):
	final_nb_cluster = 2
	sum_nb_cluster = 0
	ratio = 1.0 - float(nb_clusters) / float(nb_random_patients)
	for random_step in range(0,10):
		temp_nb_cluster = 0
		for i in range(0,nb_clusters-2):
			chance = random.uniform(0, 1)
			if chance < ratio:
				temp_nb_cluster += 1
		sum_nb_cluster += temp_nb_cluster
	final_nb_cluster += int(sum_nb_cluster / 10.0)
	return final_nb_cluster

def stratified_sampling(sampling_ratio,attribute_for_stratified_sampling,nb_random_patients,dataset_name):
	cohort_members = []
	attribute_categories_of = {}
	attribute_categories_of['age'] = ["0-18","18-24","24-34","34-44","44-49","49-55","55-150"]
	attribute_categories_of['life'] = [True,False]
	attribute_categories_of['gender'] = ["M","F"]
	for attribute_category in attribute_categories_of[attribute_for_stratified_sampling]:
		stratification_limit = int(float(nb_random_patients) * sampling_ratio / float(len(attribute_categories_of[attribute_for_stratified_sampling])))
		stratification_query = ""
		if attribute_for_stratified_sampling == "age":
			age_cat = attribute_category.split("-")
			lower_age = age_cat[0]
			higher_age = age_cat[1]
			stratification_query = "select patient_id from patients where age >= "+lower_age+" and age <= "+higher_age+" and dataset='"+dataset_name+"' order by random() limit "+str(stratification_limit)
		else:
			stratification_query = "select patient_id from patients where "+attribute_for_stratified_sampling+" = '"+attribute_category+"' and dataset='"+dataset_name+"' order by random() limit "+str(stratification_limit)
		cur.execute(stratification_query)
		rows = cur.fetchall()
		for row in rows:
			cohort_members.append(row[0])
	return cohort_members

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

