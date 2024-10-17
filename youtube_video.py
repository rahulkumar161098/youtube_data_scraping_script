import pandas as pd
from googleapiclient.discovery import build
from dotenv import load_dotenv, dotenv_values 
import os
load_dotenv()

channel_id=os.getenv('YOUTUBE_CHANNEL_ID')
api_key= os.getenv('API_KEY')

youtube = build('youtube', 'v3', developerKey=api_key)

# Function to get video details
def get_video_details(channel_id):
    # Get Uploads Playlist ID for the channel
    channel_request = youtube.channels().list(part='contentDetails', id=channel_id)
    channel_response = channel_request.execute()
    
    # Get the uploads playlist ID
    uploads_playlist_id = channel_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    # Get video details from the uploads playlist
    video_ids = []
    video_data = []
    video_comment_id=[]
    next_page_token = None
    
    while True:
        playlist_request = youtube.playlistItems().list(
            part='snippet', playlistId=uploads_playlist_id, maxResults=50, pageToken=next_page_token
        )
        # print(playlist_request)

        playlist_response = playlist_request.execute()
        # print(playlist_response)
        
        for item in playlist_response['items']:
            video_id = item['snippet']['resourceId']['videoId']
            video_ids.append(video_id)
        
        next_page_token = playlist_response.get('nextPageToken')
        if next_page_token is None:
            break
    
    # Fetch detailed information for each video
    for i in range(0, len(video_ids), 50):
        video_request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=','.join(video_ids[i:i+50])
        )
        video_response = video_request.execute()
        print(video_response)
        
        for video in video_response['items']:
            video_details={
                'Video ID': video['id'],
                'Title': video['snippet']['title'],
                'Description': video['snippet']['description'],
                'Published Date': video['snippet']['publishedAt'],
                'View Count': video['statistics'].get('viewCount', 'N/A'),
                'Like Count': video['statistics'].get('likeCount', 'N/A'),
                'Comment Count': video['statistics'].get('commentCount', 'N/A'),
                'Duration': video['contentDetails']['duration'],
                'Thumbnail URL': video['snippet']['thumbnails']['high']['url']
            }
            comments=video['statistics'].get('commentCount')
            if(int(comments)>=100):
               video_data.append(video_details)
               video_comment_id.append(video['id'])
    
    print('-----------------------------------------',len(video_data))
    print('-------------------------------------------',len(video_comment_id))
    return video_data, video_comment_id

# Function to get comments for video
def get_video_comments(video_id, max_results=100):
    
    comments_data = []
    next_page_token = None
    
    while len(comments_data) < max_results:
        comments_request = youtube.commentThreads().list(
            part='snippet,replies',
            videoId=video_id,
            maxResults=min(50, max_results - len(comments_data)),  # Fetch in chunks
            pageToken=next_page_token
        )
        comments_response = comments_request.execute()
        
        for item in comments_response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comment_id = item['snippet']['topLevelComment']['id']
            comments_data.append({
                'Video ID': video_id,
                'Comment ID': comment_id,
                'Comment Text': comment['textDisplay'],
                'Author Name': comment['authorDisplayName'],
                'Published Date': comment['publishedAt'],
                'Like Count': comment['likeCount'],
                'Reply to': None  # Top-level comment, no reply to
            })
            
            # replies if any
            if 'replies' in item:
                for reply in item['replies']['comments']:
                    reply_snippet = reply['snippet']
                    comments_data.append({
                        'Video ID': video_id,
                        'Comment ID': reply['id'],
                        'Comment Text': reply_snippet['textDisplay'],
                        'Author Name': reply_snippet['authorDisplayName'],
                        'Published Date': reply_snippet['publishedAt'],
                        'Like Count': reply_snippet['likeCount'],
                        'Reply to': comment_id  # It's a reply to the top-level comment
                    })
        
        next_page_token = comments_response.get('nextPageToken')
        if next_page_token is None:
            break
    
    return comments_data

# Main function to fetch video and comments data
def export_youtube_data_to_excel(channel_id, excel_file_path):
    # Get video details
    video_data, video_ids = get_video_details(channel_id)
    
    # Prepare video data for Excel
    video_df = pd.DataFrame(video_data)
    
    # Get comments for each video
    comments_data = []
    for video_id in video_ids:
        video_comments = get_video_comments(video_id)
        comments_data.extend(video_comments)
        print('data saving in csv file...........')
    
    
    comments_df = pd.DataFrame(comments_data)
    
    # Write both to Excel in separate sheets
    with pd.ExcelWriter(excel_file_path, engine='openpyxl') as writer:
        video_df.to_excel(writer, sheet_name='Video Data', index=False)
        comments_df.to_excel(writer, sheet_name='Comments Data', index=False)


if __name__ == "__main__": 
    print ("Data collecting......")
    export_youtube_data_to_excel(channel_id, 'youtube_channel_data.xlsx')
    print("Data exported successfully.")
else: 
    print ("Something went wrong.")



