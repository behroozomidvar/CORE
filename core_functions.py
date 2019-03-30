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