import tweepy
import pandas as pd


access_key = "iUX7PM5BZ5fPBlRBvhHNSZ7fG"
access_secret = "9KxD5HWFb8GgFNR5zffnAcmipyX0k8mfj7HdWGPgdoz9DoCbhk"
consumer_key = "1752696332257734656-2q3jSI1cAEQZy8cOhBu4qFqy3NReid"
consumer_secret = "ECcF0KGW4H7LVvLmjljfC16iTEJhTh4BtwiLt2bEJcLJ7"


auth = tweepy.OAuthHandler(access_key, access_secret)
auth.set_access_token(consumer_key, consumer_secret)
api = tweepy.API(auth)


target_username = "@Deena91374053"


user = api.get_user(username=target_username).data


user_data = {
    "username": user.username,
    "name": user.name,
    "bio": user.description,
    "follower_count": user.public_metrics["follower_count"],
    "following_count": user.public_metrics["following_count"],
    "tweet_count": user.public_metrics["tweet_count"],
    "profile_image_url": user.profile_image_url
}

#
df = pd.DataFrame([user_data])


df.to_csv("user_info.csv")

print(f"Fetched user information for {target_username}. Check user_info.csv for details.")
