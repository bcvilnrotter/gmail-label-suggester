# %%
# import modules
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import sys,argparse,os,base64,re,itertools
from datetime import datetime,timezone
from collections import Counter
from lxml import html
from nltk import ngrams
import nltk

# %%
# download the specific packages for helping navigate through the n-gram part of the analysis
nltk.download('punkt')

# Define the scopes that will be used when interacting with your gmail account
SCOPES = [
    'https://www.googleapis.com/auth/gmail.readonly',
    'https://www.googleapis.com/auth/gmail.settings.basic'
]

# %%
# region argparse
# Prepare the argparse section for the script to function
parser = argparse.ArgumentParser()

# Add the required arguments
parser.add_argument('--credentials',help='path to the credential json file created from google cloud project')
parser.add_argument('--labels',nargs='+',help='a string or list of label names that the script will iterate through')

# Add the optional arguments. All should have a default value
parser.add_argument('--num_common_phrases',default=3,help="a number that will be used to tell the script how many most common phrases to make into a filter query")
parser.add_argument('--num_common_phrases_pulled',default=10,help="a number that will be used to tell the script how many top common phrases to look for in the pulled email collection")
parser.add_argument('--num_phrase_compare_window',default=4,help="the window size of the ngram to compare within pulled email content")

args = parser.parse_args()

# error out if the user did not provide a link to the credentials JSON file
if not args.credentials:
    parser.error("Please provide the google creds using --credentials")

# error out if the user did not provide a label to search for
if not args.labels:
    parser.error("Please provide a label that the script can use to pull emails from using --label")

#endregion
# %%
#region basic_functions

# the get_now function will be used to ensure that a consistent timestamp would be provided to all items that require it (like logging)
def get_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d-T%H-%M-%S")

# the main log function that will be used to log activity to make sure that the user is updated on what the script is doing
def log(message,type='info'):

    # create a message string to print to the console and provide to the log of this activity
    entry = str(get_now()) + f" [{type}] {message}"

    # create the path for the log that will be written to. It should be in the same location as where the script reside when run
    logpath = os.path.join(os.getcwd(),'log.txt')

    # open or create the file defined by the logpath variable and write to it and the console
    with open(logpath,"a") as f:
        f.write(entry+"\n")
        print(entry)

# function to provide the ability of giving a spinner to a long process
def spinner():
    spinner_cycle = itertools.cycle(['|','/','-','\\'])
    while True:
        yield next(spinner_cycle)

#endregion
# %%
#region helper_functions

# function used to authenticate the session using a credential file
def authenticate_gmail(creds_file):
    creds = None
    flow = InstalledAppFlow.from_client_secrets_file(creds_file,SCOPES)
    creds = flow.run_local_server(port=0)
    service = build('gmail','v1',credentials=creds)
    return service

# function to get the labelId for the label name provided by the user. This function should be used in conjunction with get_emails, but can be used elsewhere
def get_label_id(service,label_name):
    results = service.users().labels().list(userId='me').execute()
    labels = results.get('labels',[])

    for label in labels:
        if label['name'].lower() == label_name.lower():
            return label['id']
    
    raise ValueError(f"Label '{label_name}' not found.")

# function to clean the body of the messages of html tags
def clean_html(raw_html):
    tree = html.fromstring(raw_html)

    # Remove <style> and <script> content
    for element in tree.xpath('//style|//script'):
        element.drop_tree()
    
    # Extract clean text
    clean_text=tree.text_content().strip()
    clean_text=re.sub(r'\s+',' ',clean_text)
    return clean_text
"""
def clean_html(raw_html):
    clean_text = re.sub(r'<[^>]+>','',raw_html)
    clean_text = unescape(clean_text)
    clean_text = re.sub(r'\s+',' ',clean_text).strip()
    return clean_text
"""

# function to collect the emails identified by the user (based on email used to log in using credentials and label string)
def get_emails(service,label):

    # iterate through the results and clean the body of the messages and return a giant string containt all the body data
    # need to add pagination
    email_contents=[]
    page_token = None
    total_messages = 0
    page_num = 0
    local_spinner = spinner()

    while True:
        # pull the information from the gmail API service
        results = service.users().messages().list(userId='me',labelIds=[label],pageToken=page_token).execute()
        messages = results.get('messages',[])
        num_messages = len(messages)

        if num_messages == 0: # break if no messages are returned
            break
    
        for i, message in enumerate(messages):
            msg = service.users().messages().get(userId='me',id=message['id']).execute()

            if 'data' in msg['payload']['body']:
                email_content = base64.urlsafe_b64decode(msg['payload']['body']['data'].encode('ASCII')).decode('utf-8')

            try:
                clean_content = clean_html(email_content)
                email_contents.append(clean_content)
            except Exception as e:
                log(f"   |- Error decoding message {message['id']}: {e}")
                continue

            sys.stdout.write(f"\r{next(local_spinner)} Processing emails.. Processed {total_messages} | Processing {i+1}/{num_messages}\r")
            sys.stdout.flush()

        page_token = results.get('nextPageToken')        
        if not page_token:
            sys.stdout.flush()
            break
        total_messages += num_messages
        page_num += 1

    return email_contents

# function to analyze a large string of information to pull out the most common phrase using n-grams
# n is the number of words to compare sequentially in a string
# n_common is the number of most common phraases found
def analyze_email_content(email_content,n=4,n_common=10):
    email_content = re.sub(r'\s+',' ',email_content).strip() # remove excess whitespace
    words = [word for word in re.findall(r'\b\w{3,}\b', email_content.lower())]
    n_grams = ngrams(words,n)
    common_phrases = Counter(n_grams).most_common(n_common)
    return common_phrases

#endregion
# %%
# main function initiation and call
def main():
    
    # authenticate the session using the credentials provided by the project
    service = authenticate_gmail(args.credentials)
    log('- service authenticated.')

    # identify the number of labels provided, and then iterate through them
    log(f'- {len(args.labels)} identified label name to run analysis on.')
    # create and build out a dictionary to contain all the label name:id pairs requested for iteration
    label_dict = {}
    for label in list(args.labels):
        label_dict.update({label:get_label_id(service,label)})
    log(f'- converted provided label names to a dict of label name and id pairs')

    for label in label_dict:
        log(f'|- working on [{label}] label.')
        email_contents = get_emails(service,label_dict[label])
        log(f'|- content from {len(email_contents)} messages were pulled.')
        # convert list of email contents into a giant string for easier n-gram analysis
        common_phrases = analyze_email_content(' '.join(email_contents),n=args.num_phrase_compare_window,n_common=args.num_common_phrases_pulled)
        # create a condition string that can be used in a filter
        #filter_query = ' OR '.join(['"{} {}"'.format(phrase[0][0],phrase[0][1]) for phrase in common_phrases[:args.num_common_phrases]])
        #TODO Need to 
        log(f'|- filter query created:')
        log(f'   |- [{common_phrases}]')
        #create_gmail_filter

if __name__ == "__main__":
    main()