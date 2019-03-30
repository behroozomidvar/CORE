import events

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