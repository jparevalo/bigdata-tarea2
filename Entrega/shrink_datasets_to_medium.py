import json

dataset_path = '../dataset/'

# We started creating business_medium through terminal (first 30000 business)
business_ids = list()
with open(dataset_path + 'business_medium.json', 'r') as f:
    for business_data in f:
       business = json.loads(business_data)
       business_ids.append(business['business_id'])

print 'Shrinking Reviews ...'

reviews_medium = list()
with open(dataset_path+'review.json', 'r') as f:
    for review_data in f:
        review = json.loads(review_data)
        if review['business_id'] in business_ids:
            reviews_medium.append(review)
        if len(reviews_medium) == 30000:
            break

with open(dataset_path + 'review_medium.json', 'w') as outfile:
    json.dump(reviews_medium, outfile)

print 'Finished Shrinking Reviews!'
print 'Shrinking Users ...'

reviews_user_ids = list()
for review in reviews_medium:
    reviews_user_ids.append(review['user_id'])

reviews_user_ids = set(reviews_user_ids)

users_medium = list()
with open(dataset_path+'user.json', 'r') as f:
    for user_data in f:
        user = json.loads(user_data)
        if user['user_id'] in reviews_user_ids:
            users_medium.append(user)
        elif len(users_medium) == reviews_user_ids:
            users_medium.append(user)
        if len(users_medium) == 30000:
            break

with open(dataset_path + 'user_medium.json', 'w') as outfile:
    json.dump(users_medium, outfile)

print 'Finished Shrinking Users!'
print 'Shrinking Photos ...'

photos_medium = list()
with open(dataset_path+'photos.json', 'r') as f:
    for photo_data in f:
        photo = json.loads(photo_data)
        if photo['business_id'] in business_ids:
            photos_medium.append(photo)
        if len(photos_medium) == 30000:
            break

with open(dataset_path + 'photos_medium.json', 'w') as outfile:
    json.dump(photos_medium, outfile)

print 'Finished Shrinking Photos!'
print 'Shrinking Tips ...'

tips_medium = list()
with open(dataset_path+'tip.json', 'r') as f:
    for tip_data in f:
        tip = json.loads(tip_data)
        if tip['business_id'] in business_ids:
            tips_medium.append(tip)
        if len(tips_medium) == 30000:
            break

with open(dataset_path + 'tip_medium.json', 'w') as outfile:
    json.dump(tips_medium, outfile)

print 'Finished Shrinking Tips!'
print 'Shrinking Checkins ...'

checkins_medium = list()
with open(dataset_path+'checkin.json', 'r') as f:
    for checkin_data in f:
        checkin = json.loads(checkin_data)
        if checkin['business_id'] in business_ids:
            checkins_medium.append(checkin)
        if len(checkins_medium) == 30000:
            break

with open(dataset_path + 'checkin_medium.json', 'w') as outfile:
    json.dump(checkins_medium, outfile)

print 'Finished Shrinking Checkins!'
