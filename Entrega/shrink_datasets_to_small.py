import json

dataset_path = '../dataset/'

# We started creating business_small through terminal (first 1000 business)
business_ids = list()
with open(dataset_path + 'business_small.json', 'r') as f:
    for business_data in f:
       business = json.loads(business_data)
       business_ids.append(business['business_id'])

print 'Shrinking Reviews ...'

reviews_small = list()
with open(dataset_path+'review.json', 'r') as f:
    for review_data in f:
        review = json.loads(review_data)
        if review['business_id'] in business_ids:
            reviews_small.append(review)
        if len(reviews_small) == 10000:
            break

with open(dataset_path + 'review_small.json', 'w') as outfile:
    json.dump(reviews_small, outfile)

print 'Finished Shrinking Reviews!'
print 'Shrinking Users ...'

reviews_user_ids = list()
for review in reviews_small:
    reviews_user_ids.append(review['user_id'])

users_small = list()
with open(dataset_path+'user.json', 'r') as f:
    for user_data in f:
        user = json.loads(user_data)
        if user['user_id'] in reviews_user_ids:
            users_small.append(user)
        if len(users_small) == 10000:
            break

with open(dataset_path + 'user_small.json', 'w') as outfile:
    json.dump(users_small, outfile)

print 'Finished Shrinking Users!'
print 'Shrinking Photos ...'

photos_small = list()
with open(dataset_path+'photos.json', 'r') as f:
    for photo_data in f:
        photo = json.loads(photo_data)
        if photo['business_id'] in business_ids:
            photos_small.append(photo)
        if len(photos_small) == 10000:
            break

with open(dataset_path + 'photos_small.json', 'w') as outfile:
    json.dump(photos_small, outfile)

print 'Finished Shrinking Photos!'
print 'Shrinking Tips ...'

tips_small = list()
with open(dataset_path+'tip.json', 'r') as f:
    for tip_data in f:
        tip = json.loads(tip_data)
        if tip['business_id'] in business_ids:
            tips_small.append(tip)
        if len(tips_small) == 10000:
            break

with open(dataset_path + 'tip_small.json', 'w') as outfile:
    json.dump(tips_small, outfile)

print 'Finished Shrinking Tips!'
print 'Shrinking Checkins ...'

checkins_small = list()
with open(dataset_path+'checkin.json', 'r') as f:
    for checkin_data in f:
        checkin = json.loads(checkin_data)
        if checkin['business_id'] in business_ids:
            checkins_small.append(checkin)
        if len(checkins_small) == 10000:
            break

with open(dataset_path + 'checkin_small.json', 'w') as outfile:
    json.dump(checkins_small, outfile)

print 'Finished Shrinking Checkins!'
