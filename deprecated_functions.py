def sim_users(patient_id_1, patient_id_2, cohort_based, cohort_behavior = {}, cohort_action_names = [], cohort_last_month = 0, cohort_size=0):
	targets= []
	sorted_events_p1 = retrieve_trajectories(patient_id_1)
	sorted_events_p2 = retrieve_trajectories(patient_id_2)
	event_cnt = 0
	last_month = sorted_events_p1[-1].month # with the following if condition, "last_month" keep the length of the shorter trajectory
	if last_month < sorted_events_p2[-1].month:
		last_month = sorted_events_p2[-1].month
	sum_local_similarities = 0
	main_event_cnt = 0
	for main_event in sorted_events_p1:
		main_event_cnt += 1
		sub_event_cnt = 0
		for sub_event in sorted_events_p2:
			sub_event_cnt += 1
			if sub_event.visited == True:
					continue
			if sub_event.type == main_event.type and sub_event.name == main_event.name:
				targets.append(sub_event.month)
				sub_event.visited = True
				if cohort_based == True:
					last_month = cohort_last_month
				if last_month == 0:
					print "Error: Last month is equal to zero. Impossible to compare patients."
					exit(0)
				local_similarity = float(last_month - abs(main_event.month-sub_event.month)) / last_month
				sum_local_similarities += local_similarity
				if cohort_based == False:
					print main_event, "->", sub_event, "sim:", local_similarity
				# update cohort behavior - begin
				if cohort_based == True:
					which_cell = abs(main_event.month+sub_event.month)/2
					new_behavior = CohortEvent(main_event.type,main_event.name,float(local_similarity) / float(cohort_size))
					cohort_behavior[which_cell].append(new_behavior)
					cohort_action_names.append(main_event.name)
				# update cohort behavior - end
				break
	crossings = 0
	prev_target = -1
	for target in targets:
		if target < prev_target:
			crossings += 1
		prev_target = target
	if cohort_based == False:
		print "- number of crossings:", crossings
	weighted_similarities = (1.0 - crossing_weight) * (float(sum_local_similarities)) / float(len(sorted_events_p1))
	weighted_crossing = crossing_weight * (float(len(sorted_events_p1)-1) - crossings) / (float(len(sorted_events_p1))-1)
	return weighted_similarities + weighted_crossing